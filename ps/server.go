package ps

import (
	"context"
	"fmt"
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
	// 1. etcd 노드 등록 시작 (백그라운드)
	node := domain.NodeInfo{
		ID:      s.cfg.NodeID,
		Address: s.cfg.Addr,
		Status:  domain.NodeStatusActive,
	}
	go s.registry.Register(ctx, node) //nolint:errcheck

	// 2. 라우팅 테이블 watch 시작 + 초기값 수신 대기
	rtCh := s.rtStore.Watch(ctx)
	firstRT, ok := <-rtCh
	if !ok {
		return fmt.Errorf("routing table watch closed before receiving initial value")
	}
	s.routing.Store(firstRT) // nil이어도 저장 (빈 클러스터 허용)

	// 3. 이후 라우팅 갱신을 백그라운드에서 처리
	go s.watchRouting(ctx, rtCh)

	// 4. gRPC 서버 시작
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Addr, err)
	}
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- s.grpcSrv.Serve(lis)
	}()

	// 5. EvictionScheduler 시작
	evictSched := engine.NewEvictionScheduler[Req, Resp](s.actorHost, s.cfg.IdleTimeout, s.cfg.EvictInterval)
	go evictSched.Start(ctx)

	// 6. CheckpointScheduler 시작
	cpSched := engine.NewCheckpointScheduler[Req, Resp](s.actorHost, s.cfg.CheckpointInterval)
	go cpSched.Start(ctx)

	// 7. ctx 취소 대기
	select {
	case <-ctx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("grpc server error: %w", err)
	}

	// 8. Graceful shutdown
	return s.shutdown()
}

func (s *Server[Req, Resp]) watchRouting(ctx context.Context, ch <-chan *domain.RoutingTable) {
	for rt := range ch {
		s.routing.Store(rt)
	}
}

func (s *Server[Req, Resp]) shutdown() error {
	// 1. gRPC: 진행 중인 RPC 완료 대기 후 중단
	s.grpcSrv.GracefulStop()

	// 2. 모든 Actor checkpoint 저장 (별도 timeout context 사용)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	if err := s.actorHost.EvictAll(shutdownCtx); err != nil {
		// eviction 실패는 로그만 남기고 계속 진행
		_ = err
	}

	// 3. etcd lease 즉시 revoke
	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deregCancel()
	_ = s.registry.Deregister(deregCtx, s.cfg.NodeID)

	// 4. etcd 클라이언트 종료
	return s.etcdCli.Close()
}
