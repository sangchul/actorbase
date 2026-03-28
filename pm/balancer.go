package pm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
)

// balancerRunner calls policy.Evaluate on every check_interval and executes the returned actions.
// The load-balancing decision logic is delegated to the provider.BalancePolicy implementation.
type balancerRunner struct {
	cfg          *policy.RunnerConfig
	pol          provider.BalancePolicy
	splitter     pmSplitter
	migrator     pmMigrator
	merger       pmMerger
	nodeCatalog  cluster.NodeCatalog
	routingStore cluster.RoutingTableStore
	psFactory    transport.PSClientFactory
	opMu         *sync.Mutex

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
	nodeCatalog  cluster.NodeCatalog,
	routingStore cluster.RoutingTableStore,
	psFactory transport.PSClientFactory,
	opMu *sync.Mutex,
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
		opMu:                opMu,
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
	// Only pass Active nodes to the policy; Waiting/Failed/Draining nodes
	// should not be considered for load-balancing decisions.
	nodes := make([]domain.NodeInfo, 0, len(allNodes))
	for _, n := range allNodes {
		if n.Status == domain.NodeStatusActive {
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
			partitionsByNode[entry.Node.ID] = append(partitionsByNode[entry.Node.ID], pi)
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

// executeActions executes the list of actions returned by the policy in order.
func (r *balancerRunner) executeActions(ctx context.Context, actions []provider.BalanceAction) {
	for _, action := range actions {
		switch action.Type {
		case provider.ActionSplit:
			slog.Info("autoBalancer: split triggered",
				"partition", action.PartitionID, "actor_type", action.ActorType)
			r.opMu.Lock()
			newID, err := r.splitter.Split(ctx, action.ActorType, action.PartitionID, "", "")
			r.opMu.Unlock()
			if err != nil {
				slog.Error("autoBalancer: split failed", "partition", action.PartitionID, "err", err)
				continue
			}
			slog.Info("autoBalancer: split done", "partition", action.PartitionID, "new_partition", newID)
			r.markAction(action.PartitionID)

		case provider.ActionMigrate:
			slog.Info("autoBalancer: migrate triggered",
				"partition", action.PartitionID, "target", action.TargetNode)
			r.opMu.Lock()
			err := r.migrator.Migrate(ctx, action.ActorType, action.PartitionID, action.TargetNode)
			r.opMu.Unlock()
			if err != nil {
				slog.Error("autoBalancer: migrate failed", "partition", action.PartitionID, "err", err)
				continue
			}
			slog.Info("autoBalancer: migrate done", "partition", action.PartitionID, "to", action.TargetNode)
			r.markAction(action.PartitionID)

		case provider.ActionFailover:
			slog.Info("autoBalancer: failover triggered",
				"partition", action.PartitionID, "target", action.TargetNode)
			r.opMu.Lock()
			err := r.migrator.Failover(ctx, action.PartitionID, action.TargetNode)
			r.opMu.Unlock()
			if err != nil {
				slog.Error("autoBalancer: failover failed", "partition", action.PartitionID, "err", err)
				continue
			}
			slog.Info("autoBalancer: failover done", "partition", action.PartitionID, "to", action.TargetNode)

		case provider.ActionMerge:
			slog.Info("autoBalancer: merge triggered",
				"lower", action.PartitionID, "upper", action.MergeTarget, "actor_type", action.ActorType)
			r.opMu.Lock()
			err := r.ensureSameNodeAndMerge(ctx, action)
			r.opMu.Unlock()
			if err != nil {
				slog.Error("autoBalancer: merge failed",
					"lower", action.PartitionID, "upper", action.MergeTarget, "err", err)
				continue
			}
			slog.Info("autoBalancer: merge done",
				"lower", action.PartitionID, "upper", action.MergeTarget)
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

// ensureSameNodeAndMerge checks whether lower and upper are on the same node;
// if not, migrates upper to the lower's node before executing the merge.
// opMu must be held by the caller.
func (r *balancerRunner) ensureSameNodeAndMerge(ctx context.Context, action provider.BalanceAction) error {
	rt, err := r.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return fmt.Errorf("load routing table: %w", err)
	}

	lowerEntry, ok := rt.LookupByPartition(action.PartitionID)
	if !ok {
		return fmt.Errorf("lower partition %s not found", action.PartitionID)
	}
	upperEntry, ok := rt.LookupByPartition(action.MergeTarget)
	if !ok {
		return fmt.Errorf("upper partition %s not found", action.MergeTarget)
	}

	// If they are on different nodes, migrate upper to the lower's node.
	if lowerEntry.Node.ID != upperEntry.Node.ID {
		slog.Info("autoBalancer: merge: migrating upper to lower's node",
			"upper", action.MergeTarget, "from", upperEntry.Node.ID, "to", lowerEntry.Node.ID)
		if err := r.migrator.Migrate(ctx, action.ActorType, action.MergeTarget, lowerEntry.Node.ID); err != nil {
			return fmt.Errorf("migrate upper to same node: %w", err)
		}
	}

	return r.merger.Merge(ctx, action.ActorType, action.PartitionID, action.MergeTarget)
}

// errDeadNode is a sentinel error used inside buildClusterStats to mark a dead node.
var errDeadNode = errors.New("dead node")
