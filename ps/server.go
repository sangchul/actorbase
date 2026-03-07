package ps

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/oomymy/actorbase/internal/cluster"
	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/engine"
	"github.com/oomymy/actorbase/internal/transport"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
)

// Server는 Partition Server의 진입점.
// Config를 받아 내부 컴포넌트를 조립하고 gRPC 서버를 기동·관리한다.
type Server[Req, Resp any] struct {
	cfg       Config[Req, Resp]
	actorHost *engine.ActorHost[Req, Resp]
	registry  cluster.NodeRegistry
	rtStore   cluster.RoutingTableStore
	grpcSrv   *grpc.Server
	etcdCli   *clientv3.Client
	routing   atomic.Pointer[domain.RoutingTable] // 현재 라우팅 테이블. lock-free 읽기.
}

// NewServer는 Config를 검증하고 컴포넌트를 조립한다.
// etcd 연결, gRPC 서버 생성까지 수행하지만 Start 호출 전까지 수신은 시작하지 않는다.
func NewServer[Req, Resp any](cfg Config[Req, Resp]) (*Server[Req, Resp], error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// etcd 클라이언트 생성
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	// ActorHost 생성
	actorHost := engine.NewActorHost[Req, Resp](engine.Config[Req, Resp]{
		Factory:                cfg.Factory,
		WALStore:               cfg.WALStore,
		CheckpointStore:        cfg.CheckpointStore,
		Metrics:                cfg.Metrics,
		MailboxSize:            cfg.MailboxSize,
		FlushSize:              cfg.FlushSize,
		FlushInterval:          cfg.FlushInterval,
		CheckpointWALThreshold: cfg.CheckpointWALThreshold,
	})

	// cluster 컴포넌트 생성
	registry := cluster.NewNodeRegistry(etcdCli, cfg.EtcdLeaseTTL)
	rtStore := cluster.NewRoutingTableStore(etcdCli)

	// gRPC 서버 생성
	grpcSrv := transport.NewGRPCServer(transport.ServerConfig{
		ListenAddr: cfg.Addr,
		Metrics:    cfg.Metrics,
	})

	s := &Server[Req, Resp]{
		cfg:       cfg,
		actorHost: actorHost,
		registry:  registry,
		rtStore:   rtStore,
		grpcSrv:   grpcSrv,
		etcdCli:   etcdCli,
	}

	// gRPC 핸들러 등록
	pb.RegisterPartitionServiceServer(grpcSrv, &partitionHandler[Req, Resp]{
		host:    actorHost,
		routing: &s.routing,
		codec:   cfg.Codec,
		nodeID:  cfg.NodeID,
	})
	pb.RegisterPartitionControlServiceServer(grpcSrv, &controlHandler[Req, Resp]{
		host:   actorHost,
		nodeID: cfg.NodeID,
	})

	return s, nil
}

// Start는 PS를 기동한다. ctx 취소 시 graceful shutdown 후 반환한다.
func (s *Server[Req, Resp]) Start(ctx context.Context) error {
	// 1. PM이 기동 중인지 확인 (etcd에 PM presence 키가 있어야 함)
	if err := cluster.WaitForPM(ctx, s.etcdCli); err != nil {
		return err
	}

	// 2. etcd 노드 등록 시작 (백그라운드)
	node := domain.NodeInfo{
		ID:      s.cfg.NodeID,
		Address: s.cfg.Addr,
		Status:  domain.NodeStatusActive,
	}
	go s.registry.Register(ctx, node) //nolint:errcheck

	// 3. 라우팅 테이블 watch 시작 + 초기값 수신 대기
	rtCh := s.rtStore.Watch(ctx)
	firstRT, ok := <-rtCh
	if !ok {
		return fmt.Errorf("routing table watch closed before receiving initial value")
	}
	s.routing.Store(firstRT) // nil이어도 저장 (빈 클러스터 허용)

	// 4. 이후 라우팅 갱신을 백그라운드에서 처리
	go s.watchRouting(ctx, rtCh)

	// 5. gRPC 서버 시작
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Addr, err)
	}
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- s.grpcSrv.Serve(lis)
	}()

	// 6. EvictionScheduler 시작
	evictSched := engine.NewEvictionScheduler[Req, Resp](s.actorHost, s.cfg.IdleTimeout, s.cfg.EvictInterval)
	go evictSched.Start(ctx)

	// 7. CheckpointScheduler 시작
	cpSched := engine.NewCheckpointScheduler[Req, Resp](s.actorHost, s.cfg.CheckpointInterval)
	go cpSched.Start(ctx)

	// 8. ctx 취소 대기
	select {
	case <-ctx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("grpc server error: %w", err)
	}

	// 9. Graceful shutdown
	return s.shutdown()
}

func (s *Server[Req, Resp]) watchRouting(ctx context.Context, ch <-chan *domain.RoutingTable) {
	for rt := range ch {
		s.routing.Store(rt)
	}
}

func (s *Server[Req, Resp]) shutdown() error {
	// 1. 파티션 선이전: 가용한 다른 PS로 migrate 요청.
	// gRPC 서버가 살아있는 동안 실행해야 PM이 ExecuteMigrateOut을 호출할 수 있다.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), s.cfg.DrainTimeout)
	defer drainCancel()
	s.drainPartitions(drainCtx)

	// 2. gRPC: 진행 중인 RPC 완료 대기 후 중단
	s.grpcSrv.GracefulStop()

	// 3. 모든 Actor checkpoint 저장 (drain 실패 파티션 대비 safety)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	if err := s.actorHost.EvictAll(shutdownCtx); err != nil {
		_ = err
	}

	// 4. etcd lease 즉시 revoke
	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deregCancel()
	_ = s.registry.Deregister(deregCtx, s.cfg.NodeID)

	// 5. etcd 클라이언트 종료
	return s.etcdCli.Close()
}

// drainPartitions는 이 PS가 담당하는 모든 파티션을 다른 PS로 migrate 요청한다.
// 일부 실패해도 계속 진행한다. 실패한 파티션은 etcd lease 만료 후 PM의 failoverNode가 처리한다.
func (s *Server[Req, Resp]) drainPartitions(ctx context.Context) {
	// 1. etcd에서 PM 주소 조회
	pmAddr, err := cluster.GetPMAddr(ctx, s.etcdCli)
	if err != nil {
		slog.Warn("ps: drain: cannot get PM address, skipping drain", "err", err)
		return
	}

	// 2. PM 연결
	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	pmConn, err := pool.Get(pmAddr)
	if err != nil {
		slog.Warn("ps: drain: cannot connect to PM", "pm_addr", pmAddr, "err", err)
		return
	}
	pmClient := transport.NewPMClient(pmConn)

	// 3. 가용 노드 목록 조회
	members, err := pmClient.ListMembers(ctx)
	if err != nil {
		slog.Warn("ps: drain: cannot list members", "err", err)
		return
	}
	targets := make([]transport.MemberInfo, 0, len(members))
	for _, m := range members {
		if m.NodeID != s.cfg.NodeID && m.Status == domain.NodeStatusActive {
			targets = append(targets, m)
		}
	}
	if len(targets) == 0 {
		slog.Warn("ps: drain: no available target nodes, skipping drain")
		return
	}

	// 4. 현재 라우팅에서 내 파티션 추출
	rt := s.routing.Load()
	if rt == nil {
		return
	}
	var i int
	for _, entry := range rt.Entries() {
		if entry.Node.ID != s.cfg.NodeID {
			continue
		}
		target := targets[i%len(targets)]
		i++
		if err := pmClient.RequestMigrate(ctx, entry.Partition.ID, target.NodeID); err != nil {
			slog.Error("ps: drain: migrate failed",
				"partition", entry.Partition.ID, "target", target.NodeID, "err", err)
		} else {
			slog.Info("ps: drain: partition migrated",
				"partition", entry.Partition.ID, "target", target.NodeID)
		}
	}
}
