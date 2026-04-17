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
	"github.com/sangchul/actorbase/pm/taskqueue"
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
	queue        *taskqueue.Queue

	// Current routing table. Delivered immediately when a new WatchRouting stream connects.
	routing atomic.Pointer[domain.RoutingTable]

	// WatchRouting subscriber management.
	subsMu      sync.RWMutex
	subscribers map[string]*subscriber // clientID → subscriber

	// YAML-based policy state. nil means AutoPolicy is inactive.
	policyMu         sync.RWMutex
	activePolicy     provider.BalancePolicy // Policy implementation applied via YAML.
	activeRunnerCfg  *policy.RunnerConfig   // Parameters for the balancerRunner.
	activePolicyYAML string                 // Raw YAML stored in etcd (returned by GetPolicy).
	balancerCancel   context.CancelFunc     // Cancels the balancerRunner goroutine.

	// serverCtx stores the ctx from Start() and is used in applyPolicy for the balancer lifetime.
	serverCtx context.Context

	// heartbeats tracks the last heartbeat time per PS nodeID.
	// Updated by the Heartbeat RPC handler; read by runFailureDetector.
	heartbeats sync.Map // nodeID → time.Time

	// evictedNodes tracks nodes that have sent EvictionComplete.
	// Allows PM to skip walFlushMargin and immediately assign partition to new PS.
	evictedNodes sync.Map // nodeID → struct{}

	// bootstrapOnce ensures the initial routing table is created exactly once,
	// even if multiple PS nodes send RequestJoin concurrently on a fresh cluster.
	bootstrapOnce sync.Once
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

	nodeRegistry := cluster.NewNodeRegistry(etcdCli, 10*time.Second)
	nodeCatalog := cluster.NewNodeCatalog(etcdCli)
	membership := cluster.NewMembershipWatcher(etcdCli)
	workJournal := cluster.NewWorkJournal(etcdCli)

	var redisCli goredis.UniversalClient
	var routingStore cluster.RoutingTableStore
	if cfg.RedisAddr != "" {
		redisCli = goredis.NewUniversalClient(&goredis.UniversalOptions{
			Addrs: []string{cfg.RedisAddr},
		})
		routingStore = cluster.NewRedisRoutingTableStore(redisCli)
	} else {
		routingStore = cluster.NewRoutingTableStore(etcdCli)
	}
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
		redisCli:     redisCli, // nil if RedisAddr is empty (etcd mode)
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
		queue:        taskqueue.New(),
		subscribers:  make(map[string]*subscriber),
	}

	pb.RegisterPartitionManagerServiceServer(grpcSrv, &managerHandler{server: s})

	return s, nil
}

// Start starts the PM. Returns after graceful shutdown when ctx is cancelled.
// In HA mode, blocks until this instance wins the etcd leader election.
// Only the elected leader opens the gRPC port and starts serving.
func (s *Server) Start(ctx context.Context) error {
	// serverCtx is set to leaderCtx below, after session is acquired.

	// 1. etcd election — block until this instance becomes the leader (standbys wait here).
	sess, err := cluster.CampaignLeader(ctx, s.etcdCli, s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("pm: leader election: %w", err)
	}
	defer sess.Close() //nolint:errcheck — resign on shutdown

	// leaderCtx is cancelled when EITHER the caller cancels ctx (normal shutdown)
	// OR the etcd session expires (lease lost due to GC pause / network isolation).
	// All goroutines and the task queue use leaderCtx so they stop immediately when
	// leadership is lost, preventing a stale PM from issuing directives after a new
	// leader has been elected.
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	s.serverCtx = leaderCtx
	defer leaderCancel()
	go func() {
		select {
		case <-sess.Done():
			slog.Error("pm: etcd session expired — leadership lost, shutting down",
				"addr", s.cfg.ListenAddr)
			leaderCancel()
		case <-leaderCtx.Done():
		}
	}()

	// 2. Start the task queue worker (before balancer and resumePendingWork so they can submit tasks).
	go s.queue.Start(leaderCtx, s.executeTask)

	// 3. Restore YAML policy from Redis or etcd.
	if yamlStr, err := s.loadPolicy(leaderCtx); err != nil {
		slog.Warn("pm: load policy failed", "err", err)
	} else if yamlStr != "" {
		if pol, runnerCfg, parseErr := policy.ParsePolicy([]byte(yamlStr)); parseErr != nil {
			slog.Warn("pm: stored policy parse failed", "err", parseErr)
		} else {
			balancerCtx, cancel := context.WithCancel(leaderCtx)
			b := newBalancerRunner(runnerCfg, pol, s.splitter, s.migrator, s.merger, s.nodeCatalog, s.routingStore, s.psFactory, s.queue)
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

	// 4. Reconcile catalog: ping Active/Draining nodes; non-responsive → Waiting.
	// This handles the case where PM crashed while a PS was offline.
	s.reconcileCatalogByPing(leaderCtx)
	// Recover partitions left on non-Active nodes after PM crash + PS failure.
	s.recoverOrphanedPartitions(leaderCtx)

	// 5. Load the initial routing table.
	currentRT, err := s.routingStore.Load(leaderCtx)
	if err != nil {
		return fmt.Errorf("pm: load routing table: %w", err)
	}
	s.routing.Store(currentRT)

	// 6. Watch routing store and broadcast updates to subscribers.
	go s.watchRouting(leaderCtx)

	// 7. Watch node join events (NodeLeft is now handled by the failure detector).
	go s.watchMembership(leaderCtx)

	// 7b. Start failure detector: triggers handleNodeLeft for nodes whose heartbeats time out.
	go s.runFailureDetector(leaderCtx)

	// 8. Resume any in-progress work that was interrupted by a PM crash.
	s.resumePendingWork(leaderCtx)

	// 9. Start the web console HTTP server if configured.
	// (Bootstrap of the initial routing table is handled lazily in handleNodeJoined
	//  when the first PS calls RequestJoin on a fresh cluster.)
	if s.cfg.HTTPAddr != "" {
		consoleSrv := console.NewServer(s.cfg.HTTPAddr, s.cfg.ListenAddr)
		go consoleSrv.Start(leaderCtx)
	}

	// 11. Start accepting gRPC connections.
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("pm: listen %s: %w", s.cfg.ListenAddr, err)
	}
	grpcErrCh := make(chan error, 1)
	go func() {
		grpcErrCh <- s.grpcSrv.Serve(lis)
	}()

	select {
	case <-leaderCtx.Done():
	case err := <-grpcErrCh:
		return fmt.Errorf("pm: grpc server error: %w", err)
	}

	s.grpcSrv.GracefulStop()
	s.connPool.Close() //nolint:errcheck
	return nil
}

// reconcileCatalogByPing verifies Active/Draining/Drained/Restricted nodes by pinging them directly.
// Nodes that do not respond are reset to Waiting.
// Called once on PM startup (after leader election). Replaces the old etcd-heartbeat-based reconcile.
func (s *Server) reconcileCatalogByPing(ctx context.Context) {
	nodes, err := s.nodeCatalog.ListNodes(ctx)
	if err != nil {
		slog.Warn("pm: reconcile: failed to list catalog nodes", "err", err)
		return
	}

	for _, n := range nodes {
		switch n.Status {
		case domain.NodeStatusActive, domain.NodeStatusDraining,
			domain.NodeStatusDrained, domain.NodeStatusRestricted:
			// These states imply the node should be running — verify by ping.
		default:
			continue
		}
		if n.Address == "" {
			continue
		}
		pingCtx, cancel := context.WithTimeout(ctx, s.cfg.PingTimeout)
		alive := s.pingPS(pingCtx, n.Address)
		cancel()
		if alive {
			continue
		}
		// Node claims to be running but does not respond — reset to Waiting.
		if err := s.nodeCatalog.UpdateStatus(ctx, n.ID, domain.NodeStatusWaiting); err != nil {
			slog.Warn("pm: reconcile: failed to reset node", "node", n.ID, "err", err)
		} else {
			slog.Info("pm: reconcile: reset unresponsive node to Waiting", "node", n.ID, "prev_status", n.Status)
		}
	}
}

// runFailureDetector periodically checks heartbeat timestamps.
// Any Active node whose last heartbeat exceeds HeartbeatTimeout is declared dead and triggers handleNodeLeft.
func (s *Server) runFailureDetector(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.checkHeartbeats(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) checkHeartbeats(ctx context.Context) {
	threshold := time.Now().Add(-s.cfg.HeartbeatTimeout)
	var stale []string
	s.heartbeats.Range(func(key, val any) bool {
		if val.(time.Time).Before(threshold) {
			stale = append(stale, key.(string))
		}
		return true
	})
	for _, nodeID := range stale {
		s.heartbeats.Delete(nodeID)
		node, found, err := s.nodeCatalog.GetNode(ctx, nodeID)
		if err != nil || !found {
			continue
		}
		switch node.Status {
		case domain.NodeStatusActive, domain.NodeStatusRestricted:
			slog.Warn("pm: failure detector: heartbeat timeout, declaring node dead",
				"node", nodeID, "timeout", s.cfg.HeartbeatTimeout)
			go s.handleNodeLeft(ctx, node, cluster.NodeLeaveFailure)
		case domain.NodeStatusDraining:
			// PS crashed during drain; treat as failure so partitions are recovered.
			slog.Warn("pm: failure detector: heartbeat timeout on Draining node, treating as failure",
				"node", nodeID, "timeout", s.cfg.HeartbeatTimeout)
			go s.handleNodeLeft(ctx, node, cluster.NodeLeaveFailure)
		default:
			continue
		}
	}
}

// waitForEviction blocks until the node sends EvictionComplete or walFlushMargin elapses.
// Ensures the dead PS has flushed its WAL to shared storage before the replacement PS reads it.
func (s *Server) waitForEviction(ctx context.Context, nodeID string) {
	deadline := time.Now().Add(s.cfg.WalFlushMargin)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, done := s.evictedNodes.LoadAndDelete(nodeID); done {
			slog.Info("pm: waitForEviction: EvictionComplete received, proceeding immediately", "node", nodeID)
			return
		}
		if time.Now().After(deadline) {
			slog.Info("pm: waitForEviction: walFlushMargin elapsed, proceeding", "node", nodeID,
				"margin", s.cfg.WalFlushMargin)
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
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

// watchMembership handles node join events from the etcd membership watcher.
// NodeLeft events are no longer sourced from etcd; the failure detector handles them.
func (s *Server) watchMembership(ctx context.Context) {
	slog.Info("pm: membership watcher started")
	ch := s.membership.Watch(ctx)
	for event := range ch {
		switch event.Type {
		case cluster.NodeJoined:
			slog.Info("pm: node joined (etcd)", "node", event.Node.ID, "addr", event.Node.Address)
			go s.handleNodeJoined(ctx, event.Node)
		case cluster.NodeLeft:
			// NodeLeft from etcd (TTL expiry) is now a secondary signal.
			// The failure detector is the primary source; ignore duplicate events.
			slog.Debug("pm: node left (etcd TTL, secondary signal — already handled by failure detector)",
				"node", event.Node.ID)
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

	nodes, err := s.operationalNodes(ctx)
	if err != nil {
		slog.Error("pm: handleNodeJoined: list nodes failed", "node", node.ID, "err", err)
		return
	}
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		slog.Error("pm: handleNodeJoined: load routing table failed", "node", node.ID, "err", err)
		return
	}

	// If no routing table exists yet, this is the first node of a fresh cluster.
	// Bootstrap by creating the initial routing table for each actor type.
	if rt == nil {
		s.bootstrapFirstNode(ctx, node)
		return
	}

	pol := s.activeBalancePolicy()
	stats := s.quickClusterStats(ctx, nodes, rt, "")
	actions := pol.OnNodeJoined(ctx, domainToProviderNodeInfo(node), stats)
	slog.Info("pm: handleNodeJoined: policy returned actions", "node", node.ID, "actions", len(actions))
	s.executeBalanceActions(ctx, actions)

	// Recover any partitions left on non-Active nodes (Failed or Waiting-but-offline).
	// This handles cases where a PS died with no other PS available at the time.
	s.recoverOrphanedPartitions(ctx)
}

// bootstrapFirstNode creates the initial routing table for a fresh cluster.
// Uses sync.Once to guard against concurrent RequestJoins on startup.
func (s *Server) bootstrapFirstNode(ctx context.Context, firstNode domain.NodeInfo) {
	s.bootstrapOnce.Do(func() {
		// Re-check under the lock in case another goroutine already bootstrapped.
		if existing, err := s.routingStore.Load(ctx); err != nil || existing != nil {
			if existing != nil {
				slog.Info("pm bootstrap: routing table already exists, skipping", "node", firstNode.ID)
			}
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
				NodeID:          firstNode.ID,
				PartitionStatus: domain.PartitionStatusActive,
				Epoch:           1,
			})
		}

		initial, err := domain.NewRoutingTable(1, entries)
		if err != nil {
			slog.Error("pm bootstrap: create routing table", "err", err)
			return
		}
		initial.WithNodeAddrs(map[string]string{firstNode.ID: firstNode.Address})
		if err := s.routingStore.Save(ctx, initial); err != nil {
			slog.Error("pm bootstrap: save routing table", "err", err)
			return
		}
		slog.Info("pm bootstrap: initial routing table created",
			"actor_types", s.cfg.ActorTypes, "node", firstNode.ID)
	})
}

func (s *Server) handleNodeLeft(ctx context.Context, node domain.NodeInfo, reason cluster.NodeLeaveReason) {
	// Note: the false-positive ping check has been removed.
	// Failure is now detected via PM-direct heartbeat timeout (runFailureDetector),
	// which is more accurate than etcd TTL expiry. No ping is needed.

	// Determine the catalog state to decide how to handle the departure.
	catalogEntry, found, _ := s.nodeCatalog.GetNode(ctx, node.ID)
	wasDraining := found && catalogEntry.Status == domain.NodeStatusDraining
	isDrained   := found && catalogEntry.Status == domain.NodeStatusDrained

	nodes, err := s.operationalNodes(ctx)
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
			if e.NodeID == node.ID {
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

	if isDrained {
		// Drain completed successfully before the node exited.
		// No failover needed — partitions were already migrated during drain.
		if err := s.nodeCatalog.UpdateStatus(ctx, node.ID, domain.NodeStatusWaiting); err != nil {
			slog.Error("pm: handleNodeLeft: failed to set Waiting after completed drain", "node", node.ID, "err", err)
		} else {
			slog.Info("pm: node returned to Waiting after completed drain", "node", node.ID)
		}
	} else if wasDraining {
		// Graceful shutdown: drainPartitions already migrated the partitions.
		// Failover any stragglers (usually none), then transition back to Waiting.
		s.failoverDeadNode(ctx, node.ID)
		if err := s.nodeCatalog.UpdateStatus(ctx, node.ID, domain.NodeStatusWaiting); err != nil {
			slog.Error("pm: handleNodeLeft: failed to set Waiting after drain", "node", node.ID, "err", err)
		} else {
			slog.Info("pm: node returned to Waiting after graceful drain", "node", node.ID)
		}
	} else if deadPartitions == 0 {
		// Unexpected failure with no partitions: no data at risk, safe to auto-recover.
		// Node can rejoin without operator intervention.
		if err := s.nodeCatalog.UpdateStatus(ctx, node.ID, domain.NodeStatusWaiting); err != nil {
			slog.Error("pm: handleNodeLeft: failed to set Waiting (no partitions)", "node", node.ID, "err", err)
		} else {
			slog.Info("pm: node with no partitions returned to Waiting after unexpected exit", "node", node.ID)
		}
	} else {
		// Unexpected failure with partitions: wait for WAL flush before assigning to new PS.
		// Either EvictionComplete arrives (fast path) or walFlushMargin elapses (safe path).
		s.waitForEviction(ctx, node.ID)
		// Operator review required before rejoin.
		if err := s.nodeCatalog.UpdateStatus(ctx, node.ID, domain.NodeStatusFailed); err != nil {
			slog.Error("pm: handleNodeLeft: failed to set Failed", "node", node.ID, "err", err)
		}
		s.failoverDeadNode(ctx, node.ID)
		slog.Info("pm: node marked Failed; run 'abctl node reset' to allow rejoin", "node", node.ID)
	}
}

// pingPS sends a single gRPC Ping to the PS at addr.
// Returns true if the PS is alive (responded successfully within the deadline set by the caller).
func (s *Server) pingPS(ctx context.Context, addr string) bool {
	ctrl, err := s.psFactory.GetClient(addr)
	if err != nil {
		return false
	}
	return ctrl.Ping(ctx) == nil
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
		if entry.NodeID == deadNodeID {
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
		taskID := s.queue.Submit(taskqueue.PriorityFailover, "failover", entry.Partition.ActorType, entry.Partition.ID,
			failoverTaskParams{PartitionID: entry.Partition.ID, TargetNodeID: target.ID})
		ferr := s.queue.Wait(ctx, taskID)
		if ferr != nil {
			slog.Error("pm: failoverDeadNode: failover failed",
				"partition", entry.Partition.ID, "target", target.ID, "err", ferr)
		} else {
			slog.Info("pm: failoverDeadNode: failover complete",
				"partition", entry.Partition.ID, "to", target.ID)
		}
	}
}

// recoverOrphanedPartitions reassigns partitions that belong to non-Active nodes to currently Active nodes.
// Uses Failover (skips ExecuteMigrateOut) because the source node may be offline.
// Called on node join (case E/F) and on PM start after reconcileCatalog (crash recovery).
func (s *Server) recoverOrphanedPartitions(ctx context.Context) {
	rt, err := s.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return
	}

	activeNodes, err := s.activeNodes(ctx)
	if err != nil || len(activeNodes) == 0 {
		return
	}

	activeSet := make(map[string]bool, len(activeNodes))
	for _, n := range activeNodes {
		activeSet[n.ID] = true
	}

	var orphaned []domain.RouteEntry
	for _, entry := range rt.Entries() {
		if !activeSet[entry.NodeID] {
			orphaned = append(orphaned, entry)
		}
	}
	if len(orphaned) == 0 {
		return
	}

	slog.Info("pm: recoverOrphanedPartitions: found orphaned partitions", "count", len(orphaned))
	for i, entry := range orphaned {
		target := activeNodes[i%len(activeNodes)]
		slog.Info("pm: recoverOrphanedPartitions: recovering partition",
			"partition", entry.Partition.ID, "from", entry.NodeID, "to", target.ID)
		taskID := s.queue.Submit(taskqueue.PriorityFailover, "failover",
			entry.Partition.ActorType, entry.Partition.ID,
			failoverTaskParams{PartitionID: entry.Partition.ID, TargetNodeID: target.ID})
		if ferr := s.queue.Wait(ctx, taskID); ferr != nil {
			slog.Error("pm: recoverOrphanedPartitions: failover failed",
				"partition", entry.Partition.ID, "target", target.ID, "err", ferr)
		} else {
			slog.Info("pm: recoverOrphanedPartitions: partition recovered",
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
// Used as migration targets (failover, recoverOrphanedPartitions).
// Restricted nodes are intentionally excluded: they do not accept new partitions.
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

// operationalNodes returns Active and Restricted nodes from the catalog.
// Used to build ClusterStats passed to BalancePolicy. Restricted nodes hold
// partitions and must appear in stats, but the policy must not pick them as
// migration targets (checked in pickLeastLoaded via provider.NodeStatusRestricted).
func (s *Server) operationalNodes(ctx context.Context) ([]domain.NodeInfo, error) {
	all, err := s.nodeCatalog.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]domain.NodeInfo, 0, len(all))
	for _, n := range all {
		if n.Status == domain.NodeStatusActive || n.Status == domain.NodeStatusRestricted {
			result = append(result, n)
		}
	}
	return result, nil
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
			partitionsByNode[entry.NodeID] = append(partitionsByNode[entry.NodeID], pi)
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

// executeBalanceActions executes the actions returned by BalancePolicy via the task queue.
// Failover actions are submitted at PriorityFailover; others at PriorityAuto.
// Each action blocks until completion to preserve ordering within a single event handler call.
func (s *Server) executeBalanceActions(ctx context.Context, actions []provider.BalanceAction) {
	for _, action := range actions {
		var taskID string
		switch action.Type {
		case provider.ActionSplit:
			slog.Info("pm: executing split", "actor_type", action.ActorType, "partition", action.PartitionID)
			taskID = s.queue.Submit(taskqueue.PriorityAuto, "split", action.ActorType, action.PartitionID,
				splitTaskParams{ActorType: action.ActorType, PartitionID: action.PartitionID})
		case provider.ActionMigrate:
			slog.Info("pm: executing migrate", "actor_type", action.ActorType, "partition", action.PartitionID, "target", action.TargetNode)
			taskID = s.queue.Submit(taskqueue.PriorityAuto, "migrate", action.ActorType, action.PartitionID,
				migrateTaskParams{ActorType: action.ActorType, PartitionID: action.PartitionID, TargetNodeID: action.TargetNode})
		case provider.ActionFailover:
			slog.Info("pm: executing failover", "partition", action.PartitionID, "target", action.TargetNode)
			taskID = s.queue.Submit(taskqueue.PriorityFailover, "failover", action.ActorType, action.PartitionID,
				failoverTaskParams{PartitionID: action.PartitionID, TargetNodeID: action.TargetNode})
		case provider.ActionMerge:
			slog.Info("pm: executing merge", "actor_type", action.ActorType, "lower", action.PartitionID, "upper", action.MergeTarget)
			taskID = s.queue.Submit(taskqueue.PriorityAuto, "merge", action.ActorType, action.PartitionID,
				mergeTaskParams{ActorType: action.ActorType, LowerID: action.PartitionID, UpperID: action.MergeTarget})
		default:
			continue
		}
		if err := s.queue.Wait(ctx, taskID); err != nil {
			slog.Error("pm: balance action failed", "type", action.Type, "partition", action.PartitionID, "err", err)
		}
	}
}

func domainToProviderNodeInfo(n domain.NodeInfo) provider.NodeInfo {
	var pStatus provider.NodeStatus
	switch n.Status {
	case domain.NodeStatusDraining:
		pStatus = provider.NodeStatusDraining
	case domain.NodeStatusRestricted:
		pStatus = provider.NodeStatusRestricted
	default:
		pStatus = provider.NodeStatusActive
	}
	return provider.NodeInfo{
		ID:     n.ID,
		Addr:   n.Address,
		Status: pStatus,
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
	if err := s.savePolicy(ctx, yamlStr); err != nil {
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
	b := newBalancerRunner(runnerCfg, pol, s.splitter, s.migrator, s.merger, s.nodeCatalog, s.routingStore, s.psFactory, s.queue)
	go b.start(balancerCtx)
	s.policyMu.Unlock()
	slog.Info("pm: AutoPolicy applied", "check_interval", runnerCfg.CheckInterval)
	return nil
}

// clearPolicy removes the YAML policy and reverts to cfg.BalancePolicy (the code-injected policy).
func (s *Server) clearPolicy(ctx context.Context) error {
	if err := s.clearPolicyStore(ctx); err != nil {
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

// ── Task queue params and executor ───────────────────────────────────────────

// Task parameter types used with the task queue.
// These are type-switched in executeTask to dispatch to the right do* method.

type splitTaskParams struct {
	ActorType   string
	PartitionID string
	SplitKey    string
}

type migrateTaskParams struct {
	ActorType    string
	PartitionID  string
	TargetNodeID string
}

type mergeTaskParams struct {
	ActorType   string
	LowerID     string
	UpperID     string
	AutoMigrate bool // if true, migrate upper to lower's node first if they differ
}

type failoverTaskParams struct {
	PartitionID  string
	TargetNodeID string
}

// executeTask is the task queue executor — called by the single worker goroutine.
// It checks ctx before executing any work so that a cancelled leaderCtx (etcd
// session expired) prevents in-flight directives from being issued by a stale leader.
func (s *Server) executeTask(ctx context.Context, t *taskqueue.Task) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pm: task skipped (leadership lost): %w", err)
	}
	switch p := t.Params.(type) {
	case splitTaskParams:
		newID, err := s.doSplit(ctx, p.ActorType, p.PartitionID, p.SplitKey)
		t.Output = newID
		return err
	case migrateTaskParams:
		return s.doMigrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
	case mergeTaskParams:
		if p.AutoMigrate {
			return s.ensureSameNodeAndMerge(ctx, p.ActorType, p.LowerID, p.UpperID)
		}
		return s.doMerge(ctx, p.ActorType, p.LowerID, p.UpperID)
	case failoverTaskParams:
		return s.doFailover(ctx, p.PartitionID, p.TargetNodeID)
	default:
		return fmt.Errorf("pm: unknown task params type: %T", t.Params)
	}
}

// ensureSameNodeAndMerge migrates upper to the lower's node if they differ, then merges.
// Used by auto-balancer merge tasks. Called from inside the queue worker (no opMu needed).
func (s *Server) ensureSameNodeAndMerge(ctx context.Context, actorType, lowerID, upperID string) error {
	rt, err := s.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return fmt.Errorf("ensureSameNodeAndMerge: load routing table: %w", err)
	}
	lowerEntry, ok := rt.LookupByPartition(lowerID)
	if !ok {
		return fmt.Errorf("ensureSameNodeAndMerge: lower partition %s not found", lowerID)
	}
	upperEntry, ok := rt.LookupByPartition(upperID)
	if !ok {
		return fmt.Errorf("ensureSameNodeAndMerge: upper partition %s not found", upperID)
	}
	if lowerEntry.NodeID != upperEntry.NodeID {
		slog.Info("pm: merge: migrating upper to lower's node",
			"upper", upperID, "from", upperEntry.NodeID, "to", lowerEntry.NodeID)
		if err := s.migrator.Migrate(ctx, actorType, upperID, lowerEntry.NodeID); err != nil {
			return fmt.Errorf("ensureSameNodeAndMerge: migrate upper: %w", err)
		}
	}
	return s.merger.Merge(ctx, actorType, lowerID, upperID)
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
			_, splitErr := s.splitter.Split(ctx, p.ActorType, p.PartitionID, p.SplitKey, p.NewPartitionID)
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
				if !ok || entry.NodeID == p.TargetNodeID {
					_ = s.workJournal.Complete(ctx, work.ID)
					continue
				}
				if entry.PartitionStatus == domain.PartitionStatusDraining {
					// Partition stuck in Draining — use dedicated resume path that skips the check.
					err := s.migrator.ResumeMigrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
					if err != nil {
						slog.Error("pm: resumePendingWork: ResumeMigrate failed", "partition", p.PartitionID, "err", err)
					} else {
						_ = s.workJournal.Complete(ctx, work.ID)
					}
					continue
				}
			}
			migrateErr := s.migrator.Migrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
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
				if !ok || entry.NodeID == p.TargetNodeID {
					_ = s.workJournal.Complete(ctx, work.ID)
					continue
				}
				if entry.PartitionStatus == domain.PartitionStatusDraining {
					// Failover also uses ResumeMigrate — source is dead so eviction is skipped.
					err := s.migrator.ResumeMigrate(ctx, p.ActorType, p.PartitionID, p.TargetNodeID)
					if err != nil {
						slog.Error("pm: resumePendingWork: ResumeMigrate (failover) failed", "partition", p.PartitionID, "err", err)
					} else {
						_ = s.workJournal.Complete(ctx, work.ID)
					}
					continue
				}
			}
			failoverErr := s.migrator.Failover(ctx, p.PartitionID, p.TargetNodeID)
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
					err := s.merger.ResumeMerge(ctx, p.ActorType, p.LowerID, p.UpperID)
					if err != nil {
						slog.Error("pm: resumePendingWork: ResumeMerge failed", "lower", p.LowerID, "upper", p.UpperID, "err", err)
					} else {
						_ = s.workJournal.Complete(ctx, work.ID)
					}
					continue
				}
			}
			mergeErr := s.merger.Merge(ctx, p.ActorType, p.LowerID, p.UpperID)
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

// ── Policy store helpers (Redis or etcd depending on cfg.RedisAddr) ──────────

func (s *Server) loadPolicy(ctx context.Context) (string, error) {
	if s.redisCli != nil {
		return cluster.LoadRedisPolicy(ctx, s.redisCli)
	}
	return cluster.LoadPolicy(ctx, s.etcdCli)
}

func (s *Server) savePolicy(ctx context.Context, yamlStr string) error {
	if s.redisCli != nil {
		return cluster.SaveRedisPolicy(ctx, s.redisCli, yamlStr)
	}
	return cluster.SavePolicy(ctx, s.etcdCli, yamlStr)
}

func (s *Server) clearPolicyStore(ctx context.Context) error {
	if s.redisCli != nil {
		return cluster.ClearRedisPolicy(ctx, s.redisCli)
	}
	return cluster.ClearPolicy(ctx, s.etcdCli)
}
