package pm

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/pm/taskqueue"
	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
)


// balancerRunner calls policy.Evaluate on every check_interval and submits the returned actions
// to the shared task queue. The load-balancing decision logic is delegated to provider.BalancePolicy.
type balancerRunner struct {
	cfg          *policy.RunnerConfig
	pol          provider.BalancePolicy
	splitter     pmSplitter
	migrator     pmMigrator
	merger       pmMerger
	nodeCatalog  cluster.NodeCatalog
	routingStore cluster.RoutingTableStore
	psFactory    transport.PSClientFactory
	queue        *taskqueue.Queue

	mu                  sync.Mutex
	globalLastAction    time.Time
	partitionLastAction map[string]time.Time
}

func newBalancerRunner(
	cfg *policy.RunnerConfig,
	pol provider.BalancePolicy,
	splitter pmSplitter,
	migrator pmMigrator,
	merger pmMerger,
	nodeCatalog cluster.NodeCatalog,
	routingStore cluster.RoutingTableStore,
	psFactory transport.PSClientFactory,
	queue *taskqueue.Queue,
) *balancerRunner {
	return &balancerRunner{
		cfg:                 cfg,
		pol:                 pol,
		splitter:            splitter,
		migrator:            migrator,
		merger:              merger,
		nodeCatalog:         nodeCatalog,
		routingStore:        routingStore,
		psFactory:           psFactory,
		queue:               queue,
		partitionLastAction: make(map[string]time.Time),
	}
}

// start runs a loop that calls runOnce on every check_interval. Exits when ctx is cancelled.
func (r *balancerRunner) start(ctx context.Context) {
	slog.Info("autoBalancer: started", "check_interval", r.cfg.CheckInterval)
	ticker := time.NewTicker(r.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.runOnce(ctx)
		case <-ctx.Done():
			slog.Info("autoBalancer: stopped")
			return
		}
	}
}

func (r *balancerRunner) runOnce(ctx context.Context) {
	allNodes, err := r.nodeCatalog.ListNodes(ctx)
	if err != nil || len(allNodes) == 0 {
		return
	}
	// Pass Active and Restricted nodes to the policy; Waiting/Failed/Draining nodes
	// should not be considered for load-balancing decisions.
	// Restricted nodes hold partitions and must appear in stats, but the policy
	// must not pick them as migration targets (enforced in pickLeastLoaded).
	nodes := make([]domain.NodeInfo, 0, len(allNodes))
	for _, n := range allNodes {
		if n.Status == domain.NodeStatusActive || n.Status == domain.NodeStatusRestricted {
			nodes = append(nodes, n)
		}
	}
	_ = err // already checked above
	if len(nodes) == 0 {
		return
	}
	rt, err := r.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return
	}

	// Check global cooldown.
	r.mu.Lock()
	cooldown := time.Since(r.globalLastAction) < r.cfg.Cooldown.Global
	r.mu.Unlock()
	if cooldown {
		return
	}

	stats := r.buildClusterStats(ctx, nodes, rt, "")
	actions := r.pol.Evaluate(ctx, stats)
	r.executeActions(ctx, actions)
}

// buildClusterStats constructs ClusterStats.
// If deadNodeID is non-empty, that node is populated from the routing table without calling GetStats.
func (r *balancerRunner) buildClusterStats(ctx context.Context, nodes []domain.NodeInfo, rt *domain.RoutingTable, deadNodeID string) provider.ClusterStats {
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	type fetchResult struct {
		nodeInfo domain.NodeInfo
		resp     *pb.GetStatsResponse
		err      error
	}

	results := make([]fetchResult, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		if n.ID == deadNodeID {
			results[i] = fetchResult{nodeInfo: n, err: errDeadNode}
			continue
		}
		wg.Add(1)
		go func(idx int, node domain.NodeInfo) {
			defer wg.Done()
			psCtrl, connErr := r.psFactory.GetClient(node.Address)
			if connErr != nil {
				results[idx] = fetchResult{nodeInfo: node, err: connErr}
				return
			}
			resp, statsErr := psCtrl.GetStats(fetchCtx)
			results[idx] = fetchResult{nodeInfo: node, resp: resp, err: statsErr}
		}(i, n)
	}
	wg.Wait()

	// Build partition → node mapping from the routing table.
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

	nodeStats := make([]provider.NodeStats, 0, len(results))
	for _, res := range results {
		ns := provider.NodeStats{
			Node: provider.NodeInfo{
				ID:   res.nodeInfo.ID,
				Addr: res.nodeInfo.Address,
			},
		}
		if res.err != nil {
			// Dead node or unreachable: use the routing-table-based partition list.
			ns.Reachable = false
			ns.Partitions = partitionsByNode[res.nodeInfo.ID]
		} else if res.resp != nil {
			ns.Reachable = true
			ns.NodeRPS = float64(res.resp.NodeRps)
			rtParts := partitionsByNode[res.nodeInfo.ID]
			rtIndex := make(map[string]provider.PartitionInfo, len(rtParts))
			for _, p := range rtParts {
				rtIndex[p.PartitionID] = p
			}
			for _, p := range res.resp.Partitions {
				pi := provider.PartitionInfo{
					PartitionID: p.PartitionId,
					ActorType:   p.ActorType,
					KeyCount:    p.KeyCount,
					RPS:         float64(p.Rps),
				}
				// Supplement key range from the routing table.
				if rt, ok := rtIndex[p.PartitionId]; ok {
					pi.KeyRangeStart = rt.KeyRangeStart
					pi.KeyRangeEnd = rt.KeyRangeEnd
				}
				ns.Partitions = append(ns.Partitions, pi)
			}
			// Filter out partitions that are in cooldown.
			filtered := ns.Partitions[:0]
			for _, pi := range ns.Partitions {
				if !r.isPartitionCooldown(pi.PartitionID) {
					filtered = append(filtered, pi)
				}
			}
			ns.Partitions = filtered
		}
		nodeStats = append(nodeStats, ns)
	}
	return provider.ClusterStats{Nodes: nodeStats}
}

// executeActions submits the list of actions returned by the policy to the task queue.
// Actions are fire-and-forget (PriorityAuto); cooldown is marked on submit time.
func (r *balancerRunner) executeActions(_ context.Context, actions []provider.BalanceAction) {
	for _, action := range actions {
		switch action.Type {
		case provider.ActionSplit:
			slog.Info("autoBalancer: split triggered",
				"partition", action.PartitionID, "actor_type", action.ActorType)
			r.queue.Submit(taskqueue.PriorityAuto, "split", action.ActorType, action.PartitionID,
				splitTaskParams{ActorType: action.ActorType, PartitionID: action.PartitionID})
			r.markAction(action.PartitionID)

		case provider.ActionMigrate:
			slog.Info("autoBalancer: migrate triggered",
				"partition", action.PartitionID, "target", action.TargetNode)
			r.queue.Submit(taskqueue.PriorityAuto, "migrate", action.ActorType, action.PartitionID,
				migrateTaskParams{ActorType: action.ActorType, PartitionID: action.PartitionID, TargetNodeID: action.TargetNode})
			r.markAction(action.PartitionID)

		case provider.ActionFailover:
			slog.Info("autoBalancer: failover triggered",
				"partition", action.PartitionID, "target", action.TargetNode)
			r.queue.Submit(taskqueue.PriorityFailover, "failover", action.ActorType, action.PartitionID,
				failoverTaskParams{PartitionID: action.PartitionID, TargetNodeID: action.TargetNode})

		case provider.ActionMerge:
			slog.Info("autoBalancer: merge triggered",
				"lower", action.PartitionID, "upper", action.MergeTarget, "actor_type", action.ActorType)
			r.queue.Submit(taskqueue.PriorityAuto, "merge", action.ActorType, action.PartitionID,
				mergeTaskParams{ActorType: action.ActorType, LowerID: action.PartitionID, UpperID: action.MergeTarget, AutoMigrate: true})
			r.markAction(action.PartitionID)
		}
	}
}

func (r *balancerRunner) markAction(partitionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.globalLastAction = now
	r.partitionLastAction[partitionID] = now
}

func (r *balancerRunner) isPartitionCooldown(partitionID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.partitionLastAction[partitionID]
	return ok && time.Since(t) < r.cfg.Cooldown.Partition
}

// errDeadNode is a sentinel error used inside buildClusterStats to mark a dead node.
var errDeadNode = errors.New("dead node")
