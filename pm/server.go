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

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/rebalance"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
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

	// YAML 기반 policy 상태. nil이면 AutoPolicy 비활성.
	policyMu         sync.RWMutex
	activePolicy     provider.BalancePolicy // YAML로 적용된 policy 구현체
	activeRunnerCfg  *policy.RunnerConfig   // balancerRunner 실행 파라미터
	activePolicyYAML string                 // etcd 저장 원본 (GetPolicy 반환용)
	balancerCancel   context.CancelFunc     // balancerRunner goroutine 취소용

	// serverCtx는 Start()의 ctx를 저장해 applyPolicy에서 balancer lifetime에 사용한다.
	serverCtx context.Context
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
// HA 모드에서는 etcd election을 통해 리더가 될 때까지 블로킹한다.
// 리더가 된 PM만 gRPC 포트를 열고 실제 서비스를 시작한다.
func (s *Server) Start(ctx context.Context) error {
	s.serverCtx = ctx

	// 1. etcd election — 리더가 될 때까지 블로킹 (standby는 여기서 대기)
	sess, err := cluster.CampaignLeader(ctx, s.etcdCli, s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("pm: leader election: %w", err)
	}
	defer sess.Close() //nolint:errcheck — resign on shutdown

	// 2. etcd에서 YAML policy 복원
	if yamlStr, err := cluster.LoadPolicy(ctx, s.etcdCli); err != nil {
		slog.Warn("pm: load policy from etcd failed", "err", err)
	} else if yamlStr != "" {
		if pol, runnerCfg, parseErr := policy.ParsePolicy([]byte(yamlStr)); parseErr != nil {
			slog.Warn("pm: stored policy parse failed", "err", parseErr)
		} else {
			balancerCtx, cancel := context.WithCancel(ctx)
			b := newBalancerRunner(runnerCfg, pol, s.splitter, s.migrator, s.nodeRegistry, s.routingStore, s.connPool, &s.opMu)
			go b.start(balancerCtx)
			s.policyMu.Lock()
			s.activePolicy = pol
			s.activeRunnerCfg = runnerCfg
			s.activePolicyYAML = yamlStr
			s.balancerCancel = cancel
			s.policyMu.Unlock()
			slog.Info("pm: AutoPolicy restored from etcd", "check_interval", runnerCfg.CheckInterval)
		}
	}

	// 3. 라우팅 테이블 초기값 설정
	currentRT, err := s.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("pm: load routing table: %w", err)
	}
	s.routing.Store(currentRT)

	// 4. etcd watch → 구독자 broadcast
	go s.watchRouting(ctx)

	// 5. 노드 join/leave → BalancePolicy 호출
	go s.watchMembership(ctx)

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

	select {
	case <-ctx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("pm: grpc server error: %w", err)
	}

	s.grpcSrv.GracefulStop()
	s.connPool.Close() //nolint:errcheck
	return nil
}

func (s *Server) watchRouting(ctx context.Context) {
	ch := s.routingStore.Watch(ctx)
	for rt := range ch {
		s.routing.Store(rt)
		if rt != nil {
			s.broadcast(rt)
		}
	}
}

func (s *Server) broadcast(rt *domain.RoutingTable) {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()
	for _, sub := range s.subscribers {
		sub.latest.Store(rt)
		select {
		case sub.notify <- struct{}{}:
		default:
		}
	}
}

// watchMembership은 노드 join/leave 이벤트를 BalancePolicy에 전달하고 반환된 액션을 실행한다.
func (s *Server) watchMembership(ctx context.Context) {
	ch := s.membership.Watch(ctx)
	for event := range ch {
		switch event.Type {
		case cluster.NodeJoined:
			go s.handleNodeJoined(ctx, event.Node)
		case cluster.NodeLeft:
			go s.handleNodeLeft(ctx, event.Node, event.Reason)
		}
	}
}

func (s *Server) handleNodeJoined(ctx context.Context, node domain.NodeInfo) {
	nodes, err := s.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return
	}
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		return
	}

	// cfg.BalancePolicy (코드 주입) 우선, YAML policy 없으면 이것 사용
	pol := s.activeBalancePolicy()
	stats := s.quickClusterStats(ctx, nodes, rt, "")
	actions := pol.OnNodeJoined(ctx, domainToProviderNodeInfo(node), stats)
	s.executeBalanceActions(ctx, actions)
}

func (s *Server) handleNodeLeft(ctx context.Context, node domain.NodeInfo, reason cluster.NodeLeaveReason) {
	nodes, err := s.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return
	}
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		return
	}

	pol := s.activeBalancePolicy()
	// dead node 포함한 stats: dead node는 라우팅 테이블 기반, live 노드는 목록만
	stats := s.quickClusterStats(ctx, nodes, rt, node.ID)
	providerReason := provider.NodeLeaveReason(reason)
	actions := pol.OnNodeLeft(ctx, domainToProviderNodeInfo(node), providerReason, stats)
	s.executeBalanceActions(ctx, actions)
}

// activeBalancePolicy는 현재 활성 BalancePolicy를 반환한다.
// YAML policy가 적용 중이면 그 구현체를, 아니면 cfg.BalancePolicy를 반환한다.
func (s *Server) activeBalancePolicy() provider.BalancePolicy {
	s.policyMu.RLock()
	pol := s.activePolicy
	s.policyMu.RUnlock()
	if pol != nil {
		return pol
	}
	return s.cfg.BalancePolicy
}

// quickClusterStats는 이벤트 핸들러용 경량 ClusterStats를 구성한다.
// live 노드는 파티션 목록(라우팅 테이블 기반, RPS 없음), dead 노드도 라우팅 테이블 기반.
func (s *Server) quickClusterStats(ctx context.Context, nodes []domain.NodeInfo, rt *domain.RoutingTable, deadNodeID string) provider.ClusterStats {
	_ = ctx
	partitionsByNode := make(map[string][]provider.PartitionInfo)
	if rt != nil {
		for _, entry := range rt.Entries() {
			pi := provider.PartitionInfo{
				PartitionID:   entry.Partition.ID,
				ActorType:     entry.Partition.ActorType,
				KeyRangeStart: entry.Partition.KeyRange.Start,
				KeyRangeEnd:   entry.Partition.KeyRange.End,
				KeyCount:      -1,
			}
			partitionsByNode[entry.Node.ID] = append(partitionsByNode[entry.Node.ID], pi)
		}
	}

	nodeStats := make([]provider.NodeStats, 0, len(nodes)+1)

	// live 노드
	liveIDs := make(map[string]bool)
	for _, n := range nodes {
		liveIDs[n.ID] = true
		ns := provider.NodeStats{
			Node:       domainToProviderNodeInfo(n),
			Reachable:  n.ID != deadNodeID,
			Partitions: partitionsByNode[n.ID],
		}
		nodeStats = append(nodeStats, ns)
	}

	// dead 노드가 nodes 목록에 없으면 (이미 etcd에서 제거된 경우) 별도 추가
	if deadNodeID != "" && !liveIDs[deadNodeID] {
		nodeStats = append(nodeStats, provider.NodeStats{
			Node:       provider.NodeInfo{ID: deadNodeID},
			Reachable:  false,
			Partitions: partitionsByNode[deadNodeID],
		})
	}

	return provider.ClusterStats{Nodes: nodeStats}
}

// executeBalanceActions는 BalancePolicy가 반환한 액션을 실행한다.
func (s *Server) executeBalanceActions(ctx context.Context, actions []provider.BalanceAction) {
	for _, action := range actions {
		switch action.Type {
		case provider.ActionSplit:
			s.opMu.Lock()
			_, err := s.splitter.Split(ctx, action.ActorType, action.PartitionID, "")
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy split failed", "partition", action.PartitionID, "err", err)
			}

		case provider.ActionMigrate:
			s.opMu.Lock()
			err := s.migrator.Migrate(ctx, action.ActorType, action.PartitionID, action.TargetNode)
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy migrate failed", "partition", action.PartitionID, "err", err)
			}

		case provider.ActionFailover:
			s.opMu.Lock()
			err := s.migrator.Failover(ctx, action.PartitionID, action.TargetNode)
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy failover failed", "partition", action.PartitionID, "err", err)
			} else {
				slog.Info("pm: failover complete", "partition", action.PartitionID, "to", action.TargetNode)
			}
		}
	}
}

func domainToProviderNodeInfo(n domain.NodeInfo) provider.NodeInfo {
	return provider.NodeInfo{
		ID:     n.ID,
		Addr:   n.Address,
		Status: provider.NodeStatus(n.Status),
	}
}

func (s *Server) bootstrap(ctx context.Context) {
	ch := s.membership.Watch(ctx)
	for event := range ch {
		if event.Type != cluster.NodeJoined {
			continue
		}
		rt, err := s.routingStore.Load(ctx)
		if err != nil {
			slog.Error("pm bootstrap: load routing table", "err", err)
			return
		}
		if rt != nil {
			return
		}

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

// isAutoActive는 YAML AutoPolicy가 활성화 상태인지 반환한다.
func (s *Server) isAutoActive() bool {
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	return s.activeRunnerCfg != nil
}

// applyPolicy는 YAML 기반 policy를 적용하고 balancerRunner를 재시작한다.
// pol과 runnerCfg는 policy.ParsePolicy로부터 얻는다.
func (s *Server) applyPolicy(ctx context.Context, yamlStr string, pol provider.BalancePolicy, runnerCfg *policy.RunnerConfig) error {
	if err := cluster.SavePolicy(ctx, s.etcdCli, yamlStr); err != nil {
		return err
	}
	s.policyMu.Lock()
	if s.balancerCancel != nil {
		s.balancerCancel()
	}
	s.activePolicy = pol
	s.activeRunnerCfg = runnerCfg
	s.activePolicyYAML = yamlStr
	balancerCtx, cancel := context.WithCancel(s.serverCtx)
	s.balancerCancel = cancel
	b := newBalancerRunner(runnerCfg, pol, s.splitter, s.migrator, s.nodeRegistry, s.routingStore, s.connPool, &s.opMu)
	go b.start(balancerCtx)
	s.policyMu.Unlock()
	slog.Info("pm: AutoPolicy applied", "check_interval", runnerCfg.CheckInterval)
	return nil
}

// clearPolicy는 YAML policy를 제거한다. 이후 cfg.BalancePolicy(코드 주입)로 복귀한다.
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
	s.activeRunnerCfg = nil
	s.activePolicyYAML = ""
	s.policyMu.Unlock()
	slog.Info("pm: YAML policy cleared, using injected BalancePolicy")
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
