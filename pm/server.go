package pm

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/oomymy/actorbase/internal/cluster"
	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/rebalance"
	"github.com/oomymy/actorbase/internal/transport"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
	"github.com/oomymy/actorbase/pm/policy"
)

// Server는 Partition Manager의 진입점.
// 컴포넌트를 조립하고 gRPC 서버를 기동·관리한다.
type Server struct {
	cfg          Config
	etcdCli      *clientv3.Client
	routingStore cluster.RoutingTableStore
	nodeRegistry cluster.NodeRegistry
	membership   cluster.MembershipWatcher
	splitter     *rebalance.Splitter
	migrator     *rebalance.Migrator
	connPool     *transport.ConnPool
	grpcSrv      *grpc.Server

	// 현재 라우팅 테이블. WatchRouting 스트림 신규 연결 시 즉시 전달용.
	routing atomic.Pointer[domain.RoutingTable]

	// WatchRouting 구독자 관리
	subsMu      sync.RWMutex
	subscribers map[string]*subscriber // clientID → subscriber

	// split/migrate 직렬화. PM 단일 인스턴스이지만 동시 RPC 요청 방어.
	opMu sync.Mutex

	// policy 상태. nil이면 ManualPolicy (자동화 비활성).
	policyMu      sync.RWMutex
	activePolicy  *policy.ThresholdConfig
	activePolicyYAML string // etcd 저장 원본 (GetPolicy 반환용)
	balancerCancel context.CancelFunc // AutoBalancer goroutine 취소용 (Phase 3에서 사용)
}

// subscriber는 WatchRouting 스트림 하나의 상태를 담는다.
type subscriber struct {
	latest atomic.Pointer[domain.RoutingTable]
	notify chan struct{} // 버퍼 크기 1. 새 라우팅 테이블 도착 신호.
}

// NewServer는 Config를 검증하고 컴포넌트를 조립한다.
func NewServer(cfg Config) (*Server, error) {
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
		return nil, fmt.Errorf("pm: create etcd client: %w", err)
	}

	nodeRegistry := cluster.NewNodeRegistry(etcdCli, 10*time.Second)
	membership := cluster.NewMembershipWatcher(etcdCli)
	routingStore := cluster.NewRoutingTableStore(etcdCli)
	connPool := transport.NewConnPool()
	splitter := rebalance.NewSplitter(routingStore, connPool)
	migrator := rebalance.NewMigrator(routingStore, nodeRegistry, connPool)

	grpcSrv := transport.NewGRPCServer(transport.ServerConfig{
		ListenAddr: cfg.ListenAddr,
		Metrics:    cfg.Metrics,
	})

	s := &Server{
		cfg:          cfg,
		etcdCli:      etcdCli,
		routingStore: routingStore,
		nodeRegistry: nodeRegistry,
		membership:   membership,
		splitter:     splitter,
		migrator:     migrator,
		connPool:     connPool,
		grpcSrv:      grpcSrv,
		subscribers:  make(map[string]*subscriber),
	}

	pb.RegisterPartitionManagerServiceServer(grpcSrv, &managerHandler{server: s})

	return s, nil
}

// Start는 PM을 기동한다. ctx 취소 시 graceful shutdown 후 반환한다.
func (s *Server) Start(ctx context.Context) error {
	// 1. etcd에 PM 존재를 등록 (PS가 PM 가동 여부를 확인하는 데 사용)
	if err := cluster.RegisterPM(ctx, s.etcdCli, s.cfg.ListenAddr); err != nil {
		return fmt.Errorf("pm: register presence: %w", err)
	}

	// 2. etcd에서 policy 복원 (이전 세션에서 저장된 policy가 있으면 AutoPolicy 활성화)
	if yamlStr, err := cluster.LoadPolicy(ctx, s.etcdCli); err != nil {
		slog.Warn("pm: load policy from etcd failed, starting with ManualPolicy", "err", err)
	} else if yamlStr != "" {
		if cfg, parseErr := policy.ParsePolicy([]byte(yamlStr)); parseErr != nil {
			slog.Warn("pm: stored policy parse failed, starting with ManualPolicy", "err", parseErr)
		} else {
			balancerCtx, cancel := context.WithCancel(ctx)
			b := newAutoBalancer(cfg, s.splitter, s.migrator, s.nodeRegistry, s.routingStore, s.connPool, &s.opMu)
			go b.start(balancerCtx)
			s.policyMu.Lock()
			s.activePolicy = cfg
			s.activePolicyYAML = yamlStr
			s.balancerCancel = cancel
			s.policyMu.Unlock()
			slog.Info("pm: AutoPolicy restored from etcd", "algorithm", cfg.Algorithm)
		}
	}

	// 3. 현재 라우팅 테이블 조회 및 초기값 설정
	currentRT, err := s.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("pm: load routing table: %w", err)
	}
	s.routing.Store(currentRT)

	// 3. etcd watch → 구독자 broadcast
	go s.watchRouting(ctx)

	// 4. 노드 join/leave → Policy 호출
	go s.watchMembership(ctx)

	// 5. AutoRebalancePolicy Start (해당하는 경우)
	type starter interface{ Start(ctx context.Context) }
	if st, ok := s.cfg.Policy.(starter); ok {
		go st.Start(ctx)
	}

	// 6. 빈 클러스터면 첫 PS 등록 대기 후 초기 테이블 생성
	if currentRT == nil {
		go s.bootstrap(ctx)
	}

	// 7. gRPC 수신 시작
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("pm: listen %s: %w", s.cfg.ListenAddr, err)
	}
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- s.grpcSrv.Serve(lis)
	}()

	// 8. ctx 취소 대기
	select {
	case <-ctx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("pm: grpc server error: %w", err)
	}

	// 9. Graceful shutdown
	s.grpcSrv.GracefulStop()
	s.connPool.Close() //nolint:errcheck
	return nil
}

// watchRouting은 etcd 라우팅 테이블 변경을 감지하여 로컬 캐시를 갱신하고
// 모든 WatchRouting 구독자에게 broadcast한다.
func (s *Server) watchRouting(ctx context.Context) {
	ch := s.routingStore.Watch(ctx)
	for rt := range ch {
		s.routing.Store(rt)
		if rt != nil {
			s.broadcast(rt)
		}
	}
}

// broadcast는 새 라우팅 테이블을 모든 구독자에게 전달한다.
// 느린 구독자는 최신 값으로 덮어쓰인다.
func (s *Server) broadcast(rt *domain.RoutingTable) {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()
	for _, sub := range s.subscribers {
		sub.latest.Store(rt)
		select {
		case sub.notify <- struct{}{}:
		default: // 이미 신호가 있으면 추가 신호 불필요
		}
	}
}

// watchMembership은 노드 join/leave 이벤트를 감지하여 처리한다.
// NodeLeft 시: Policy와 무관하게 항상 failoverNode 실행 후 Policy.OnNodeLeft 호출.
func (s *Server) watchMembership(ctx context.Context) {
	ch := s.membership.Watch(ctx)
	for event := range ch {
		switch event.Type {
		case cluster.NodeJoined:
			s.cfg.Policy.OnNodeJoined(ctx, event.Node)
		case cluster.NodeLeft:
			go s.failoverNode(ctx, event.Node)
			s.cfg.Policy.OnNodeLeft(ctx, event.Node)
		}
	}
}

// failoverNode는 죽은 노드의 파티션을 다른 active 노드로 failover한다.
// Policy와 무관하게 NodeLeft 시 항상 실행된다.
// graceful drain이 완료된 PS는 이미 파티션이 없으므로 no-op.
func (s *Server) failoverNode(ctx context.Context, deadNode domain.NodeInfo) {
	s.opMu.Lock()
	defer s.opMu.Unlock()

	rt, err := s.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return
	}

	nodes, err := s.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return
	}

	for _, entry := range rt.Entries() {
		if entry.Node.ID != deadNode.ID {
			continue
		}
		target := pickTarget(nodes, deadNode.ID)
		if target == "" {
			slog.Warn("pm: failover: no available target node",
				"partition", entry.Partition.ID, "dead_node", deadNode.ID)
			continue
		}
		if err := s.migrator.Failover(ctx, entry.Partition.ID, target); err != nil {
			slog.Error("pm: failover failed",
				"partition", entry.Partition.ID, "target", target, "err", err)
		} else {
			slog.Info("pm: failover complete",
				"partition", entry.Partition.ID, "from", deadNode.ID, "to", target)
		}
	}
}

// pickTarget은 excludeNodeID를 제외한 active 노드 중 하나를 반환한다.
func pickTarget(nodes []domain.NodeInfo, excludeNodeID string) string {
	for _, n := range nodes {
		if n.ID != excludeNodeID && n.Status == domain.NodeStatusActive {
			return n.ID
		}
	}
	return ""
}

// bootstrap은 첫 번째 PS가 등록될 때까지 기다린 후 초기 라우팅 테이블을 생성한다.
// 각 actor type마다 전체 키 범위 ["", "")를 담당하는 초기 파티션을 생성한다.
// etcd Load 후 Save로 중복 생성을 방어한다.
func (s *Server) bootstrap(ctx context.Context) {
	ch := s.membership.Watch(ctx)
	for event := range ch {
		if event.Type != cluster.NodeJoined {
			continue
		}

		// CAS guard: 다른 PM이 먼저 생성했으면 종료
		rt, err := s.routingStore.Load(ctx)
		if err != nil {
			slog.Error("pm bootstrap: load routing table", "err", err)
			return
		}
		if rt != nil {
			return
		}

		// actor type마다 전체 키 범위 ["", "") → 첫 번째 PS
		entries := make([]domain.RouteEntry, 0, len(s.cfg.ActorTypes))
		for _, actorType := range s.cfg.ActorTypes {
			entries = append(entries, domain.RouteEntry{
				Partition: domain.Partition{
					ID:        uuid.New().String(),
					ActorType: actorType,
					KeyRange:  domain.KeyRange{Start: "", End: ""},
				},
				Node:            event.Node,
				PartitionStatus: domain.PartitionStatusActive,
			})
		}

		initial, err := domain.NewRoutingTable(1, entries)
		if err != nil {
			slog.Error("pm bootstrap: create routing table", "err", err)
			return
		}

		if err := s.routingStore.Save(ctx, initial); err != nil {
			slog.Error("pm bootstrap: save routing table", "err", err)
			return
		}
		slog.Info("pm bootstrap: initial routing table created",
			"actor_types", s.cfg.ActorTypes, "node", event.Node.ID)
		return
	}
}

// isAutoActive는 AutoPolicy가 활성화 상태인지 반환한다.
func (s *Server) isAutoActive() bool {
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	return s.activePolicy != nil
}

// applyPolicy는 새 정책을 적용한다. etcd 저장, 내부 상태 갱신, AutoBalancer 재시작.
func (s *Server) applyPolicy(ctx context.Context, yamlStr string, cfg *policy.ThresholdConfig) error {
	if err := cluster.SavePolicy(ctx, s.etcdCli, yamlStr); err != nil {
		return err
	}
	s.policyMu.Lock()
	// 기존 balancer 중단
	if s.balancerCancel != nil {
		s.balancerCancel()
	}
	s.activePolicy = cfg
	s.activePolicyYAML = yamlStr
	// 새 balancer 시작
	balancerCtx, cancel := context.WithCancel(ctx)
	s.balancerCancel = cancel
	b := newAutoBalancer(cfg, s.splitter, s.migrator, s.nodeRegistry, s.routingStore, s.connPool, &s.opMu)
	go b.start(balancerCtx)
	s.policyMu.Unlock()
	slog.Info("pm: AutoPolicy applied", "algorithm", cfg.Algorithm, "check_interval", cfg.CheckInterval)
	return nil
}

// clearPolicy는 정책을 제거하고 ManualPolicy로 전환한다.
func (s *Server) clearPolicy(ctx context.Context) error {
	if err := cluster.ClearPolicy(ctx, s.etcdCli); err != nil {
		return err
	}
	s.policyMu.Lock()
	if s.balancerCancel != nil {
		s.balancerCancel()
		s.balancerCancel = nil
	}
	s.activePolicy = nil
	s.activePolicyYAML = ""
	s.policyMu.Unlock()
	slog.Info("pm: policy cleared, reverted to ManualPolicy")
	return nil
}

func (s *Server) subscribe(clientID string, sub *subscriber) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	s.subscribers[clientID] = sub
}

func (s *Server) unsubscribe(clientID string) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	delete(s.subscribers, clientID)
}
