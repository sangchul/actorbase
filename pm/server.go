package pm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/rebalance"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/pm/console"
	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
)

// ── PM-internal interfaces for testability ───────────────────────────────────
// These allow unit tests to inject mock implementations of the rebalance operations.

type pmSplitter interface {
	Split(ctx context.Context, actorType, partitionID, splitKey, newPartitionID string) (string, error)
}

type pmMigrator interface {
	Migrate(ctx context.Context, actorType, partitionID, targetNodeID string) error
	Failover(ctx context.Context, partitionID, targetNodeID string) error
	ResumeMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error
}

type pmMerger interface {
	Merge(ctx context.Context, actorType, lowerID, upperID string) error
	ResumeMerge(ctx context.Context, actorType, lowerID, upperID string) error
}

// Server is the entry point for the Partition Manager.
// Assembles components and starts/manages the gRPC server.
type Server struct {
	cfg          Config
	etcdCli      *clientv3.Client
	redisCli     goredis.UniversalClient
	routingStore cluster.RoutingTableStore
	nodeRegistry cluster.NodeRegistry // heartbeat keys (liveness)
	nodeCatalog  cluster.NodeCatalog  // persistent node state (all 4 states)
	membership   cluster.MembershipWatcher
	workJournal  cluster.WorkJournal
	splitter     pmSplitter
	migrator     pmMigrator
	merger       pmMerger
	connPool     *transport.ConnPool
	psFactory    transport.PSClientFactory
	grpcSrv      *grpc.Server

	// Current routing table. Delivered immediately when a new WatchRouting stream connects.
	routing atomic.Pointer[domain.RoutingTable]

	// WatchRouting subscriber management.
	subsMu      sync.RWMutex
	subscribers map[string]*subscriber // clientID → subscriber

	// Serializes split/migrate operations. The PM is a single instance, but concurrent RPC requests must be guarded.
	opMu sync.Mutex

	// YAML-based policy state. nil means AutoPolicy is inactive.
	policyMu         sync.RWMutex
	activePolicy     provider.BalancePolicy // Policy implementation applied via YAML.
	activeRunnerCfg  *policy.RunnerConfig   // Parameters for the balancerRunner.
	activePolicyYAML string                 // Raw YAML stored in etcd (returned by GetPolicy).
	balancerCancel   context.CancelFunc     // Cancels the balancerRunner goroutine.

	// serverCtx stores the ctx from Start() and is used in applyPolicy for the balancer lifetime.
	serverCtx context.Context
}

// subscriber holds the state for a single WatchRouting stream.
type subscriber struct {
	latest atomic.Pointer[domain.RoutingTable]
	notify chan struct{} // Buffered with size 1. Signals arrival of a new routing table.
}

// NewServer validates the Config and assembles the components.
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

	redisCli := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs: []string{cfg.RedisAddr},
	})

	nodeRegistry := cluster.NewNodeRegistry(etcdCli, 10*time.Second)
	nodeCatalog := cluster.NewNodeCatalog(etcdCli)
	membership := cluster.NewMembershipWatcher(etcdCli)
	workJournal := cluster.NewWorkJournal(etcdCli)
	routingStore := cluster.NewRedisRoutingTableStore(redisCli)
	connPool := transport.NewConnPool()
	psFactory := transport.NewConnPoolFactory(connPool)
	splitter := rebalance.NewSplitter(routingStore, psFactory)
	migrator := rebalance.NewMigrator(routingStore, nodeCatalog, psFactory)
	merger := rebalance.NewMerger(routingStore, psFactory)

	grpcSrv := transport.NewGRPCServer(transport.ServerConfig{
		ListenAddr: cfg.ListenAddr,
		Metrics:    cfg.Metrics,
	})

	s := &Server{
		cfg:          cfg,
		etcdCli:      etcdCli,
		redisCli:     redisCli,
		routingStore: routingStore,
		nodeRegistry: nodeRegistry,
		nodeCatalog:  nodeCatalog,
		membership:   membership,
		workJournal:  workJournal,
		splitter:     splitter,
		migrator:     migrator,
		merger:       merger,
		connPool:     connPool,
		psFactory:    psFactory,
		grpcSrv:      grpcSrv,
		subscribers:  make(map[string]*subscriber),
	}

	pb.RegisterPartitionManagerServiceServer(grpcSrv, &managerHandler{server: s})

	return s, nil
}

// Start starts the PM. Returns after graceful shutdown when ctx is cancelled.
// In HA mode, blocks until this instance wins the etcd leader election.
// Only the elected leader opens the gRPC port and starts serving.
func (s *Server) Start(ctx context.Context) error {
	s.serverCtx = ctx

	// 1. etcd election — block until this instance becomes the leader (standbys wait here).
	sess, err := cluster.CampaignLeader(ctx, s.etcdCli, s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("pm: leader election: %w", err)
	}
	defer sess.Close() //nolint:errcheck — resign on shutdown

	// 2. Restore YAML policy from Redis.
	if yamlStr, err := cluster.LoadRedisPolicy(ctx, s.redisCli); err != nil {
		slog.Warn("pm: load policy from redis failed", "err", err)
	} else if yamlStr != "" {
		if pol, runnerCfg, parseErr := policy.ParsePolicy([]byte(yamlStr)); parseErr != nil {
			slog.Warn("pm: stored policy parse failed", "err", parseErr)
		} else {
			balancerCtx, cancel := context.WithCancel(ctx)
			b := newBalancerRunner(runnerCfg, pol, s.splitter, s.migrator, s.merger, s.nodeCatalog, s.routingStore, s.psFactory, &s.opMu)
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

	// 3. Reconcile catalog: Active/Draining nodes with no live heartbeat → Waiting.
	// This handles the case where PM crashed while PS was gracefully shutting down.
	s.reconcileCatalog(ctx)

	// 4. Load the initial routing table.
	currentRT, err := s.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("pm: load routing table: %w", err)
	}
	s.routing.Store(currentRT)

	// 5. Watch routing store and broadcast updates to subscribers.
	go s.watchRouting(ctx)

	// 6. Watch node join/leave events and invoke BalancePolicy.
	go s.watchMembership(ctx)

	// 7. Resume any in-progress work that was interrupted by a PM crash.
	s.resumePendingWork(ctx)

	// 8. If the cluster is empty, wait for the first PS to register and create the initial routing table.
	if currentRT == nil {
		go s.bootstrap(ctx)
	}

	// 9. Start the web console HTTP server if configured.
	if s.cfg.HTTPAddr != "" {
		consoleSrv := console.NewServer(s.cfg.HTTPAddr, s.cfg.ListenAddr)
		go consoleSrv.Start(ctx)
	}

	// 10. Start accepting gRPC connections.
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

// reconcileCatalog sets catalog nodes that are Active/Draining but have no live heartbeat to Waiting.
// Called once on PM startup (after leader election) to handle the case where PM was down
// when a PS gracefully shut down (leaving catalog in Active state with no heartbeat).
func (s *Server) reconcileCatalog(ctx context.Context) {
	liveIDs, err := s.nodeRegistry.ListLiveNodeIDs(ctx)
	if err != nil {
		slog.Warn("pm: reconcile: failed to list live nodes", "err", err)
		return
	}
	liveSet := make(map[string]struct{}, len(liveIDs))
	for _, id := range liveIDs {
		liveSet[id] = struct{}{}
	}

	nodes, err := s.nodeCatalog.ListNodes(ctx)
	if err != nil {
		slog.Warn("pm: reconcile: failed to list catalog nodes", "err", err)
		return
	}

	for _, n := range nodes {
		if n.Status != domain.NodeStatusActive && n.Status != domain.NodeStatusDraining {
			continue
		}
		if _, alive := liveSet[n.ID]; alive {
			continue
		}
		// Node is Active/Draining in catalog but has no live heartbeat — set to Waiting.
		if err := s.nodeCatalog.UpdateStatus(ctx, n.ID, domain.NodeStatusWaiting); err != nil {
			slog.Warn("pm: reconcile: failed to reset node", "node", n.ID, "err", err)
		} else {
			slog.Info("pm: reconcile: reset stale node to Waiting", "node", n.ID, "prev_status", n.Status)
		}
	}
}

func (s *Server) watchRouting(ctx context.Context) {
	ch := s.routingStore.Watch(ctx)
	for rt := range ch {
		s.routing.Store(rt)
		if rt != nil {
			slog.Info("pm: routing table updated", "version", rt.Version(), "partitions", len(rt.Entries()))
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

// watchMembership delivers node join/leave events to BalancePolicy and executes the returned actions.
func (s *Server) watchMembership(ctx context.Context) {
	slog.Info("pm: membership watcher started")
	ch := s.membership.Watch(ctx)
	for event := range ch {
		switch event.Type {
		case cluster.NodeJoined:
			slog.Info("pm: node joined", "node", event.Node.ID, "addr", event.Node.Address)
			go s.handleNodeJoined(ctx, event.Node)
		case cluster.NodeLeft:
			slog.Info("pm: node left", "node", event.Node.ID, "reason", event.Reason)
			go s.handleNodeLeft(ctx, event.Node, event.Reason)
		}
	}
	slog.Info("pm: membership watcher stopped")
}

func (s *Server) handleNodeJoined(ctx context.Context, node domain.NodeInfo) {
	// Safety check: the node must be Active in the catalog (set by RequestJoin).
	// Under normal flow this is always true; log a warning if not.
	if entry, found, err := s.nodeCatalog.GetNode(ctx, node.ID); err == nil {
		if !found || entry.Status != domain.NodeStatusActive {
			slog.Warn("pm: handleNodeJoined: node not Active in catalog, ignoring",
				"node", node.ID, "found", found)
			return
		}
	}

	nodes, err := s.activeNodes(ctx)
	if err != nil {
		slog.Error("pm: handleNodeJoined: list nodes failed", "node", node.ID, "err", err)
		return
	}
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		slog.Error("pm: handleNodeJoined: load routing table failed", "node", node.ID, "err", err)
		return
	}

	pol := s.activeBalancePolicy()
	stats := s.quickClusterStats(ctx, nodes, rt, "")
	actions := pol.OnNodeJoined(ctx, domainToProviderNodeInfo(node), stats)
	slog.Info("pm: handleNodeJoined: policy returned actions", "node", node.ID, "actions", len(actions))
	s.executeBalanceActions(ctx, actions)
}

func (s *Server) handleNodeLeft(ctx context.Context, node domain.NodeInfo, reason cluster.NodeLeaveReason) {
	// Determine the catalog state to decide how to handle the departure.
	catalogEntry, found, _ := s.nodeCatalog.GetNode(ctx, node.ID)
	wasDraining := found && catalogEntry.Status == domain.NodeStatusDraining

	nodes, err := s.activeNodes(ctx)
	if err != nil {
		slog.Error("pm: handleNodeLeft: list nodes failed", "node", node.ID, "err", err)
		return
	}
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		slog.Error("pm: handleNodeLeft: load routing table failed", "node", node.ID, "err", err)
		return
	}

	var deadPartitions int
	if rt != nil {
		for _, e := range rt.Entries() {
			if e.Node.ID == node.ID {
				deadPartitions++
			}
		}
	}
	slog.Info("pm: handleNodeLeft: computing failover actions",
		"node", node.ID, "reason", reason, "was_draining", wasDraining,
		"live_nodes", len(nodes), "dead_partitions", deadPartitions)

	pol := s.activeBalancePolicy()
	stats := s.quickClusterStats(ctx, nodes, rt, node.ID)
	providerReason := provider.NodeLeaveReason(reason)
	actions := pol.OnNodeLeft(ctx, domainToProviderNodeInfo(node), providerReason, stats)
	slog.Info("pm: handleNodeLeft: policy returned actions", "node", node.ID, "actions", len(actions))
	s.executeBalanceActions(ctx, actions)

	if wasDraining {
		// Graceful shutdown: drainPartitions already migrated the partitions.
		// Failover any stragglers (usually none), then transition back to Waiting.
		s.failoverDeadNode(ctx, node.ID)
		if err := s.nodeCatalog.UpdateStatus(ctx, node.ID, domain.NodeStatusWaiting); err != nil {
			slog.Error("pm: handleNodeLeft: failed to set Waiting after drain", "node", node.ID, "err", err)
		} else {
			slog.Info("pm: node returned to Waiting after graceful drain", "node", node.ID)
		}
	} else {
		// Unexpected failure: mark Failed, then failover. Node stays Failed until
		// the operator runs 'abctl node reset' to allow it to rejoin.
		if err := s.nodeCatalog.UpdateStatus(ctx, node.ID, domain.NodeStatusFailed); err != nil {
			slog.Error("pm: handleNodeLeft: failed to set Failed", "node", node.ID, "err", err)
		}
		s.failoverDeadNode(ctx, node.ID)
		slog.Info("pm: node marked Failed; run 'abctl node reset' to allow rejoin", "node", node.ID)
	}
}

// failoverDeadNode failovers any remaining partitions of the dead node to active nodes, regardless of policy.
// If the node was gracefully drained beforehand, no partitions remain in the routing table and this becomes a no-op.
func (s *Server) failoverDeadNode(ctx context.Context, deadNodeID string) {
	rt, err := s.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return
	}

	// List of remaining partitions on the dead node (empty if the policy already handled them).
	var remaining []domain.RouteEntry
	for _, entry := range rt.Entries() {
		if entry.Node.ID == deadNodeID {
			remaining = append(remaining, entry)
		}
	}
	if len(remaining) == 0 {
		slog.Info("pm: failoverDeadNode: no remaining partitions, skipping", "dead_node", deadNodeID)
		return
	}

	// Find available target nodes (Active only).
	nodes, err := s.activeNodes(ctx)
	if err != nil {
		slog.Error("pm: failoverDeadNode: list nodes failed", "err", err)
		return
	}
	var targets []domain.NodeInfo
	for _, n := range nodes {
		if n.ID != deadNodeID && n.Status == domain.NodeStatusActive {
			targets = append(targets, n)
		}
	}
	if len(targets) == 0 {
		slog.Warn("pm: failoverDeadNode: no available target nodes", "dead_node", deadNodeID, "partitions", len(remaining))
		return
	}

	slog.Info("pm: failoverDeadNode: starting", "dead_node", deadNodeID, "partitions", len(remaining), "targets", len(targets))
	for i, entry := range remaining {
		target := targets[i%len(targets)]
		slog.Info("pm: failoverDeadNode: failing over partition",
			"partition", entry.Partition.ID, "actor_type", entry.Partition.ActorType, "target", target.ID)
		s.opMu.Lock()
		ferr := s.doFailover(ctx, entry.Partition.ID, target.ID)
		s.opMu.Unlock()
		if ferr != nil {
			slog.Error("pm: failoverDeadNode: failover failed",
				"partition", entry.Partition.ID, "target", target.ID, "err", ferr)
		} else {
			slog.Info("pm: failoverDeadNode: failover complete",
				"partition", entry.Partition.ID, "to", target.ID)
		}
	}
}

// activeBalancePolicy returns the currently active BalancePolicy.
// Returns the YAML-applied implementation if one is active, otherwise falls back to cfg.BalancePolicy.
func (s *Server) activeBalancePolicy() provider.BalancePolicy {
	s.policyMu.RLock()
	pol := s.activePolicy
	s.policyMu.RUnlock()
	if pol != nil {
		return pol
	}
	return s.cfg.BalancePolicy
}

// activeNodes returns only Active nodes from the catalog.
// Used to build cluster stats and failover target lists.
func (s *Server) activeNodes(ctx context.Context) ([]domain.NodeInfo, error) {
	all, err := s.nodeCatalog.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	active := make([]domain.NodeInfo, 0, len(all))
	for _, n := range all {
		if n.Status == domain.NodeStatusActive {
			active = append(active, n)
		}
	}
	return active, nil
}

// quickClusterStats builds a lightweight ClusterStats for event handlers.
// Live nodes carry a partition list based on the routing table (no RPS); dead nodes are also routing-table-based.
// nodes should contain only Active nodes.
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

	// Live nodes.
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

	// If the dead node is no longer in the nodes list (already removed from etcd), add it separately.
	if deadNodeID != "" && !liveIDs[deadNodeID] {
		nodeStats = append(nodeStats, provider.NodeStats{
			Node:       provider.NodeInfo{ID: deadNodeID},
			Reachable:  false,
			Partitions: partitionsByNode[deadNodeID],
		})
	}

	return provider.ClusterStats{Nodes: nodeStats}
}

// executeBalanceActions executes the actions returned by BalancePolicy.
func (s *Server) executeBalanceActions(ctx context.Context, actions []provider.BalanceAction) {
	for _, action := range actions {
		switch action.Type {
		case provider.ActionSplit:
			slog.Info("pm: executing split", "actor_type", action.ActorType, "partition", action.PartitionID)
			s.opMu.Lock()
			_, err := s.doSplit(ctx, action.ActorType, action.PartitionID, "")
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy split failed", "partition", action.PartitionID, "err", err)
			} else {
				slog.Info("pm: split complete", "partition", action.PartitionID)
			}

		case provider.ActionMigrate:
			slog.Info("pm: executing migrate", "actor_type", action.ActorType, "partition", action.PartitionID, "target", action.TargetNode)
			s.opMu.Lock()
			err := s.doMigrate(ctx, action.ActorType, action.PartitionID, action.TargetNode)
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy migrate failed", "partition", action.PartitionID, "target", action.TargetNode, "err", err)
			} else {
				slog.Info("pm: migrate complete", "partition", action.PartitionID, "target", action.TargetNode)
			}

		case provider.ActionFailover:
			slog.Info("pm: executing failover", "partition", action.PartitionID, "target", action.TargetNode)
			s.opMu.Lock()
			err := s.doFailover(ctx, action.PartitionID, action.TargetNode)
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy failover failed", "partition", action.PartitionID, "target", action.TargetNode, "err", err)
			} else {
				slog.Info("pm: failover complete", "partition", action.PartitionID, "to", action.TargetNode)
			}

		case provider.ActionMerge:
			slog.Info("pm: executing merge", "actor_type", action.ActorType,
				"lower", action.PartitionID, "upper", action.MergeTarget)
			s.opMu.Lock()
			err := s.doMerge(ctx, action.ActorType, action.PartitionID, action.MergeTarget)
			s.opMu.Unlock()
			if err != nil {
				slog.Error("pm: policy merge failed",
					"lower", action.PartitionID, "upper", action.MergeTarget, "err", err)
			} else {
				slog.Info("pm: merge complete",
					"lower", action.PartitionID, "upper", action.MergeTarget)
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

// isAutoActive reports whether the YAML AutoPolicy is currently active.
func (s *Server) isAutoActive() bool {
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	return s.activeRunnerCfg != nil
}

// applyPolicy applies a YAML-based policy and restarts the balancerRunner.
// pol and runnerCfg are obtained from policy.ParsePolicy.
func (s *Server) applyPolicy(ctx context.Context, yamlStr string, pol provider.BalancePolicy, runnerCfg *policy.RunnerConfig) error {
	if err := cluster.SaveRedisPolicy(ctx, s.redisCli, yamlStr); err != nil {
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
	b := newBalancerRunner(runnerCfg, pol, s.splitter, s.migrator, s.merger, s.nodeCatalog, s.routingStore, s.psFactory, &s.opMu)
	go b.start(balancerCtx)
	s.policyMu.Unlock()
	slog.Info("pm: AutoPolicy applied", "check_interval", runnerCfg.CheckInterval)
	return nil
}

// clearPolicy removes the YAML policy and reverts to cfg.BalancePolicy (the code-injected policy).
func (s *Server) clearPolicy(ctx context.Context) error {
	if err := cluster.ClearRedisPolicy(ctx, s.redisCli); err != nil {
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

// ── WorkJournal-wrapped operation methods ────────────────────────────────────

// doSplit records a WorkJournal entry, runs Split, then marks it complete.
// The journal entry is always deleted when this function returns — success or failure.
// It only persists across process restarts (PM crash), at which point resumePendingWork
// picks it up and retries with the same newPartitionID.
func (s *Server) doSplit(ctx context.Context, actorType, partitionID, splitKey string) (string, error) {
	newPartitionID := uuid.New().String() // pre-generate for idempotent resume
	params, err := cluster.MarshalWorkParams(cluster.SplitParams{
		ActorType: actorType, PartitionID: partitionID,
		SplitKey: splitKey, NewPartitionID: newPartitionID,
	})
	if err != nil {
		return "", err
	}
	work := cluster.PendingWork{
		ID: uuid.New().String(), Type: cluster.WorkTypeSplit,
		Params: params, StartedAt: time.Now(),
	}
	if err := s.workJournal.Begin(ctx, work); err != nil {
		return "", fmt.Errorf("journal begin split: %w", err)
	}
	newID, splitErr := s.splitter.Split(ctx, actorType, partitionID, splitKey, newPartitionID)
	// Always clean up the journal — it only persists if the PM process is killed.
	if err := s.workJournal.Complete(ctx, work.ID); err != nil {
		slog.Warn("pm: journal complete split failed (non-fatal)", "work_id", work.ID, "err", err)
	}
	return newID, splitErr
}

// doMigrate records a WorkJournal entry, runs Migrate, then marks it complete.
// The journal entry is always deleted on return (success or failure).
func (s *Server) doMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
	params, err := cluster.MarshalWorkParams(cluster.MigrateParams{
		ActorType: actorType, PartitionID: partitionID, TargetNodeID: targetNodeID,
	})
	if err != nil {
		return err
	}
	work := cluster.PendingWork{
		ID: uuid.New().String(), Type: cluster.WorkTypeMigrate,
		Params: params, StartedAt: time.Now(),
	}
	if err := s.workJournal.Begin(ctx, work); err != nil {
		return fmt.Errorf("journal begin migrate: %w", err)
	}
	migrateErr := s.migrator.Migrate(ctx, actorType, partitionID, targetNodeID)
	if err := s.workJournal.Complete(ctx, work.ID); err != nil {
		slog.Warn("pm: journal complete migrate failed (non-fatal)", "work_id", work.ID, "err", err)
	}
	return migrateErr
}

// doMerge records a WorkJournal entry, runs Merge, then marks it complete.
// The journal entry is always deleted on return (success or failure).
func (s *Server) doMerge(ctx context.Context, actorType, lowerID, upperID string) error {
	params, err := cluster.MarshalWorkParams(cluster.MergeParams{
		ActorType: actorType, LowerID: lowerID, UpperID: upperID,
	})
	if err != nil {
		return err
	}
	work := cluster.PendingWork{
		ID: uuid.New().String(), Type: cluster.WorkTypeMerge,
		Params: params, StartedAt: time.Now(),
	}
	if err := s.workJournal.Begin(ctx, work); err != nil {
		return fmt.Errorf("journal begin merge: %w", err)
	}
	mergeErr := s.merger.Merge(ctx, actorType, lowerID, upperID)
	if err := s.workJournal.Complete(ctx, work.ID); err != nil {
		slog.Warn("pm: journal complete merge failed (non-fatal)", "work_id", work.ID, "err", err)
	}
	return mergeErr
}

// doFailover records a WorkJournal entry, runs Failover, then marks it complete.
// The journal entry is always deleted on return (success or failure).
func (s *Server) doFailover(ctx context.Context, partitionID, targetNodeID string) error {
	params, err := cluster.MarshalWorkParams(cluster.MigrateParams{
		PartitionID: partitionID, TargetNodeID: targetNodeID,
	})
	if err != nil {
		return err
	}
	work := cluster.PendingWork{
		ID: uuid.New().String(), Type: cluster.WorkTypeFailover,
		Params: params, StartedAt: time.Now(),
	}
	if err := s.workJournal.Begin(ctx, work); err != nil {
		return fmt.Errorf("journal begin failover: %w", err)
	}
	failoverErr := s.migrator.Failover(ctx, partitionID, targetNodeID)
	if err := s.workJournal.Complete(ctx, work.ID); err != nil {
		slog.Warn("pm: journal complete failover failed (non-fatal)", "work_id", work.ID, "err", err)
	}
	return failoverErr
}

// ── resumePendingWork ────────────────────────────────────────────────────────

// resumePendingWork checks the WorkJournal for in-progress operations that
// survived a PM crash and re-executes them. This is called once on startup,
// after leader election and initial routing table load.
func (s *Server) resumePendingWork(ctx context.Context) {
	pending, err := s.workJournal.ListPending(ctx)
	if err != nil {
		slog.Warn("pm: resumePendingWork: list failed", "err", err)
		return
	}
	if len(pending) == 0 {
		return
	}
	slog.Info("pm: resumePendingWork: found pending work", "count", len(pending))

	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		slog.Warn("pm: resumePendingWork: load routing table failed", "err", err)
		return
	}

	for _, work := range pending {
		slog.Info("pm: resumePendingWork: resuming", "work_id", work.ID, "type", work.Type, "started_at", work.StartedAt)
		switch work.Type {
		case cluster.WorkTypeSplit:
			var p cluster.SplitParams
			if err := json.Unmarshal(work.Params, &p); err != nil {
				slog.Error("pm: resumePendingWork: unmarshal split params", "err", err)
				continue
			}
			if rt != nil {
				// newPartitionID already in routing → split already completed.
				if p.NewPartitionID != "" {
					if _, ok := rt.LookupByPartition(p.NewPartitionID); ok {
						_ = s.workJournal.Complete(ctx, work.ID)
						continue
					}
				}
				// Original partition gone → split already completed (new ID tracking unavailable).
				if _, ok := rt.LookupByPartition(p.PartitionID); !ok {
					_ = s.workJournal.Complete(ctx, work.ID)
					continue
				}
			}
			// Retry with the same newPartitionID to avoid orphan partitions on PS.
			s.opMu.Lock()
			_, splitErr := s.splitter.Split(ctx, p.ActorType, p.PartitionID, p.SplitKey, p.NewPartitionID)
			s.opMu.Unlock()
			if splitErr != nil {
				slog.Error("pm: resumePendingWork: split failed", "partition", p.PartitionID, "err", splitErr)
			} else {
				_ = s.workJournal.Complete(ctx, work.ID)
			}

		case cluster.WorkTypeMigrate:
			var p cluster.MigrateParams
			if err := json.Unmarshal(work.Params, &p); err != nil {
				slog.Error("pm: resumePendingWork: unmarshal migrate params", "err", err)
				continue
			}
			if rt != nil {
				entry, ok := rt.LookupByPartition(p.PartitionID)
				if !ok || entry.Node.ID == p.TargetNodeID {
					_ = s.workJournal.Complete(ctx, work.ID)
					continue
				}
				if entry.PartitionStatus == domain.PartitionStatusDraining {
					// Partition stuck in Draining — use dedicated resume path that skips the check.
					s.opMu.Lock()
					err := s.migrator.ResumeMigrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
					s.opMu.Unlock()
					if err != nil {
						slog.Error("pm: resumePendingWork: ResumeMigrate failed", "partition", p.PartitionID, "err", err)
					} else {
						_ = s.workJournal.Complete(ctx, work.ID)
					}
					continue
				}
			}
			s.opMu.Lock()
			migrateErr := s.migrator.Migrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
			s.opMu.Unlock()
			if migrateErr != nil {
				slog.Error("pm: resumePendingWork: migrate failed", "partition", p.PartitionID, "err", migrateErr)
			} else {
				_ = s.workJournal.Complete(ctx, work.ID)
			}

		case cluster.WorkTypeFailover:
			var p cluster.MigrateParams
			if err := json.Unmarshal(work.Params, &p); err != nil {
				slog.Error("pm: resumePendingWork: unmarshal failover params", "err", err)
				continue
			}
			if rt != nil {
				entry, ok := rt.LookupByPartition(p.PartitionID)
				if !ok || entry.Node.ID == p.TargetNodeID {
					_ = s.workJournal.Complete(ctx, work.ID)
					continue
				}
				if entry.PartitionStatus == domain.PartitionStatusDraining {
					// Failover also uses ResumeMigrate — source is dead so eviction is skipped.
					s.opMu.Lock()
					err := s.migrator.ResumeMigrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
					s.opMu.Unlock()
					if err != nil {
						slog.Error("pm: resumePendingWork: ResumeMigrate (failover) failed", "partition", p.PartitionID, "err", err)
					} else {
						_ = s.workJournal.Complete(ctx, work.ID)
					}
					continue
				}
			}
			s.opMu.Lock()
			failoverErr := s.migrator.Failover(ctx, p.PartitionID, p.TargetNodeID)
			s.opMu.Unlock()
			if failoverErr != nil {
				slog.Error("pm: resumePendingWork: failover failed", "partition", p.PartitionID, "err", failoverErr)
			} else {
				_ = s.workJournal.Complete(ctx, work.ID)
			}

		case cluster.WorkTypeMerge:
			var p cluster.MergeParams
			if err := json.Unmarshal(work.Params, &p); err != nil {
				slog.Error("pm: resumePendingWork: unmarshal merge params", "err", err)
				continue
			}
			if rt != nil {
				lowerEntry, lowerOK := rt.LookupByPartition(p.LowerID)
				_, upperOK := rt.LookupByPartition(p.UpperID)
				if !upperOK {
					// Upper partition gone → merge already completed.
					_ = s.workJournal.Complete(ctx, work.ID)
					continue
				}
				if lowerOK && lowerEntry.PartitionStatus == domain.PartitionStatusDraining {
					// Both partitions stuck in Draining — use dedicated resume path.
					s.opMu.Lock()
					err := s.merger.ResumeMerge(ctx, p.ActorType, p.LowerID, p.UpperID)
					s.opMu.Unlock()
					if err != nil {
						slog.Error("pm: resumePendingWork: ResumeMerge failed", "lower", p.LowerID, "upper", p.UpperID, "err", err)
					} else {
						_ = s.workJournal.Complete(ctx, work.ID)
					}
					continue
				}
			}
			s.opMu.Lock()
			mergeErr := s.merger.Merge(ctx, p.ActorType, p.LowerID, p.UpperID)
			s.opMu.Unlock()
			if mergeErr != nil {
				slog.Error("pm: resumePendingWork: merge failed", "lower", p.LowerID, "upper", p.UpperID, "err", mergeErr)
			} else {
				_ = s.workJournal.Complete(ctx, work.ID)
			}

		default:
			slog.Warn("pm: resumePendingWork: unknown work type", "type", work.Type, "work_id", work.ID)
		}
	}
}
