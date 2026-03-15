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

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/engine"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/provider"
)

// ── actorDispatcher ───────────────────────────────────────────────────────────

// actorDispatcher는 engine.ActorHost를 type-erased로 감싸는 인터페이스.
// Send/Evict/EvictAll/Activate/Split은 byte slice 레벨에서 동작하여
// 단일 PS에서 여러 actor type을 처리할 수 있다.
type actorDispatcher interface {
	Send(ctx context.Context, partitionID string, payload []byte) ([]byte, error)
	Evict(ctx context.Context, partitionID string) error
	EvictAll(ctx context.Context) error
	Activate(ctx context.Context, partitionID string) error
	Split(ctx context.Context, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error)
	StartSchedulers(ctx context.Context, idleTimeout, evictInterval, checkpointInterval time.Duration)
	GetStats() []engine.PartitionStats
	TypeID() string
}

// typedDispatcher는 engine.ActorHost[Req, Resp]를 actorDispatcher로 감싼다.
type typedDispatcher[Req, Resp any] struct {
	typeID string
	host   *engine.ActorHost[Req, Resp]
	codec  provider.Codec
}

func newTypedDispatcher[Req, Resp any](cfg TypeConfig[Req, Resp]) *typedDispatcher[Req, Resp] {
	host := engine.NewActorHost[Req, Resp](engine.Config[Req, Resp]{
		Factory:                cfg.Factory,
		WALStore:               cfg.WALStore,
		CheckpointStore:        cfg.CheckpointStore,
		MailboxSize:            cfg.MailboxSize,
		FlushSize:              cfg.FlushSize,
		FlushInterval:          cfg.FlushInterval,
		CheckpointWALThreshold: cfg.CheckpointWALThreshold,
	})
	return &typedDispatcher[Req, Resp]{typeID: cfg.TypeID, host: host, codec: cfg.Codec}
}

func (d *typedDispatcher[Req, Resp]) Send(ctx context.Context, partitionID string, payload []byte) ([]byte, error) {
	var req Req
	if err := d.codec.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	resp, err := d.host.Send(ctx, partitionID, req)
	if err != nil {
		return nil, err
	}
	return d.codec.Marshal(resp)
}

func (d *typedDispatcher[Req, Resp]) Evict(ctx context.Context, partitionID string) error {
	return d.host.Evict(ctx, partitionID)
}

func (d *typedDispatcher[Req, Resp]) EvictAll(ctx context.Context) error {
	return d.host.EvictAll(ctx)
}

func (d *typedDispatcher[Req, Resp]) Activate(ctx context.Context, partitionID string) error {
	return d.host.Activate(ctx, partitionID)
}

func (d *typedDispatcher[Req, Resp]) Split(ctx context.Context, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error) {
	return d.host.Split(ctx, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID)
}

func (d *typedDispatcher[Req, Resp]) StartSchedulers(
	ctx context.Context,
	idleTimeout, evictInterval, checkpointInterval time.Duration,
) {
	evictSched := engine.NewEvictionScheduler[Req, Resp](d.host, idleTimeout, evictInterval)
	go evictSched.Start(ctx)
	cpSched := engine.NewCheckpointScheduler[Req, Resp](d.host, checkpointInterval)
	go cpSched.Start(ctx)
}

func (d *typedDispatcher[Req, Resp]) GetStats() []engine.PartitionStats {
	return d.host.GetStats()
}

func (d *typedDispatcher[Req, Resp]) TypeID() string {
	return d.typeID
}

// ── ServerBuilder ─────────────────────────────────────────────────────────────

// ServerBuilder는 PS 서버를 조립한다.
// NewServerBuilder로 생성 후 Register로 actor type을 등록하고 Build로 Server를 생성한다.
//
// 예시:
//
//	builder := ps.NewServerBuilder(baseConfig)
//	ps.Register(builder, ps.TypeConfig[KVReq, KVResp]{TypeID: "kv", ...})
//	srv, err := builder.Build()
type ServerBuilder struct {
	base        BaseConfig
	dispatchers map[string]actorDispatcher
}

// NewServerBuilder는 BaseConfig를 받아 ServerBuilder를 생성한다.
func NewServerBuilder(cfg BaseConfig) *ServerBuilder {
	return &ServerBuilder{
		base:        cfg,
		dispatchers: make(map[string]actorDispatcher),
	}
}

// Register는 actor type을 PS에 등록한다.
// Go 제네릭의 제약으로 메서드가 아닌 패키지 레벨 함수로 제공된다.
func Register[Req, Resp any](b *ServerBuilder, cfg TypeConfig[Req, Resp]) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if _, dup := b.dispatchers[cfg.TypeID]; dup {
		return errorf("actor type %q already registered", cfg.TypeID)
	}
	b.dispatchers[cfg.TypeID] = newTypedDispatcher(cfg)
	return nil
}

// Build는 Server를 생성한다.
func (b *ServerBuilder) Build() (*Server, error) {
	b.base.setDefaults()
	if err := b.base.validate(); err != nil {
		return nil, err
	}
	if len(b.dispatchers) == 0 {
		return nil, errorf("at least one actor type must be registered via Register()")
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   b.base.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	registry := cluster.NewNodeRegistry(etcdCli, b.base.EtcdLeaseTTL)
	rtStore := cluster.NewRoutingTableStore(etcdCli)

	grpcSrv := transport.NewGRPCServer(transport.ServerConfig{
		ListenAddr: b.base.Addr,
		Metrics:    b.base.Metrics,
	})

	s := &Server{
		cfg:         b.base,
		dispatchers: b.dispatchers,
		registry:    registry,
		rtStore:     rtStore,
		grpcSrv:     grpcSrv,
		etcdCli:     etcdCli,
	}

	pb.RegisterPartitionServiceServer(grpcSrv, &partitionHandler{
		dispatchers: b.dispatchers,
		routing:     &s.routing,
		nodeID:      b.base.NodeID,
	})
	pb.RegisterPartitionControlServiceServer(grpcSrv, &controlHandler{
		dispatchers: b.dispatchers,
		nodeID:      b.base.NodeID,
		routing:     &s.routing,
	})

	return s, nil
}

// ── Server ────────────────────────────────────────────────────────────────────

// Server는 Partition Server의 진입점.
// ServerBuilder.Build()로 생성한다.
type Server struct {
	cfg         BaseConfig
	dispatchers map[string]actorDispatcher
	registry    cluster.NodeRegistry
	rtStore     cluster.RoutingTableStore
	grpcSrv     *grpc.Server
	etcdCli     *clientv3.Client
	routing     atomic.Pointer[domain.RoutingTable]
}

// Start는 PS를 기동한다. ctx 취소 시 graceful shutdown 후 반환한다.
func (s *Server) Start(ctx context.Context) error {
	// 1. PM이 기동 중인지 확인
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
	s.routing.Store(firstRT)

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

	// 6. actor type별 스케줄러 시작
	for _, d := range s.dispatchers {
		d.StartSchedulers(ctx, s.cfg.IdleTimeout, s.cfg.EvictInterval, s.cfg.CheckpointInterval)
	}

	// 7. ctx 취소 대기
	select {
	case <-ctx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("grpc server error: %w", err)
	}

	return s.shutdown()
}

func (s *Server) watchRouting(ctx context.Context, ch <-chan *domain.RoutingTable) {
	for rt := range ch {
		s.routing.Store(rt)
	}
}

func (s *Server) shutdown() error {
	// 1. 파티션 선이전
	drainCtx, drainCancel := context.WithTimeout(context.Background(), s.cfg.DrainTimeout)
	defer drainCancel()
	s.drainPartitions(drainCtx)

	// 2. gRPC: 진행 중인 RPC 완료 대기 후 중단
	s.grpcSrv.GracefulStop()

	// 3. 모든 actor type의 Actor checkpoint 저장
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	for _, d := range s.dispatchers {
		if err := d.EvictAll(shutdownCtx); err != nil {
			_ = err
		}
	}

	// 4. etcd lease 즉시 revoke
	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deregCancel()
	_ = s.registry.Deregister(deregCtx, s.cfg.NodeID)

	// 5. etcd 클라이언트 종료
	return s.etcdCli.Close()
}

// drainPartitions는 이 PS가 담당하는 모든 파티션을 다른 PS로 migrate 요청한다.
func (s *Server) drainPartitions(ctx context.Context) {
	pmAddr, err := cluster.GetPMAddr(ctx, s.etcdCli)
	if err != nil {
		slog.Warn("ps: drain: cannot get PM address, skipping drain", "err", err)
		return
	}

	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck
	pmConn, err := pool.Get(pmAddr)
	if err != nil {
		slog.Warn("ps: drain: cannot connect to PM", "pm_addr", pmAddr, "err", err)
		return
	}
	pmClient := transport.NewPMClient(pmConn)

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
		if err := pmClient.RequestMigrate(ctx, entry.Partition.ActorType, entry.Partition.ID, target.NodeID); err != nil {
			slog.Error("ps: drain: migrate failed",
				"partition", entry.Partition.ID, "target", target.NodeID, "err", err)
		} else {
			slog.Info("ps: drain: partition migrated",
				"partition", entry.Partition.ID, "target", target.NodeID)
		}
	}
}
