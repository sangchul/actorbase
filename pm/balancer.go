package pm

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/rebalance"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
)

// balancerRunner은 check_interval마다 policy.Evaluate를 호출하고 액션을 실행한다.
// 분배 판단 로직은 provider.BalancePolicy 구현체가 담당한다.
type balancerRunner struct {
	cfg          *policy.RunnerConfig
	pol          provider.BalancePolicy
	splitter     *rebalance.Splitter
	migrator     *rebalance.Migrator
	nodeRegistry cluster.NodeRegistry
	routingStore cluster.RoutingTableStore
	connPool     *transport.ConnPool
	opMu         *sync.Mutex

	mu                  sync.Mutex
	globalLastAction    time.Time
	partitionLastAction map[string]time.Time
}

func newBalancerRunner(
	cfg *policy.RunnerConfig,
	pol provider.BalancePolicy,
	splitter *rebalance.Splitter,
	migrator *rebalance.Migrator,
	nodeRegistry cluster.NodeRegistry,
	routingStore cluster.RoutingTableStore,
	connPool *transport.ConnPool,
	opMu *sync.Mutex,
) *balancerRunner {
	return &balancerRunner{
		cfg:                 cfg,
		pol:                 pol,
		splitter:            splitter,
		migrator:            migrator,
		nodeRegistry:        nodeRegistry,
		routingStore:        routingStore,
		connPool:            connPool,
		opMu:                opMu,
		partitionLastAction: make(map[string]time.Time),
	}
}

// start는 check_interval마다 runOnce를 실행하는 루프. ctx 취소 시 종료.
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
	nodes, err := r.nodeRegistry.ListNodes(ctx)
	if err != nil || len(nodes) == 0 {
		return
	}
	rt, err := r.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return
	}

	// global cooldown 확인
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

// buildClusterStats는 ClusterStats를 구성한다.
// deadNodeID가 비어 있지 않으면 해당 노드는 GetStats 없이 라우팅 테이블 기반으로 채운다.
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
			conn, connErr := r.connPool.Get(node.Address)
			if connErr != nil {
				results[idx] = fetchResult{nodeInfo: node, err: connErr}
				return
			}
			resp, statsErr := transport.NewPSControlClient(conn).GetStats(fetchCtx)
			results[idx] = fetchResult{nodeInfo: node, resp: resp, err: statsErr}
		}(i, n)
	}
	wg.Wait()

	// 라우팅 테이블에서 파티션→노드 매핑 구성
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
			// 죽은 노드 또는 연결 불가: 라우팅 테이블 기반 파티션 목록 사용
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
				// 라우팅 테이블에서 key range 보완
				if rt, ok := rtIndex[p.PartitionId]; ok {
					pi.KeyRangeStart = rt.KeyRangeStart
					pi.KeyRangeEnd = rt.KeyRangeEnd
				}
				ns.Partitions = append(ns.Partitions, pi)
			}
			// 쿨다운 중인 파티션 필터링
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

// executeActions는 policy가 반환한 액션 목록을 순서대로 실행한다.
func (r *balancerRunner) executeActions(ctx context.Context, actions []provider.BalanceAction) {
	for _, action := range actions {
		switch action.Type {
		case provider.ActionSplit:
			slog.Info("autoBalancer: split triggered",
				"partition", action.PartitionID, "actor_type", action.ActorType, "split_key", action.SplitKey)
			r.opMu.Lock()
			newID, err := r.splitter.Split(ctx, action.ActorType, action.PartitionID, action.SplitKey)
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

// errDeadNode は buildClusterStats 내부에서 죽은 노드를 표시하는 센티넬 에러.
var errDeadNode = errors.New("dead node")
