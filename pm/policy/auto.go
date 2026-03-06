package policy

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/oomymy/actorbase/internal/cluster"
	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/rebalance"
)

// AutoRebalancePolicy는 클러스터 이벤트와 주기적 메트릭 검사를 기반으로
// split/migrate를 자동으로 수행한다.
//
// 동작 방식:
//   - OnNodeJoined: 가장 부하가 높은 파티션들을 새 노드로 migrate하여 균등 분배.
//   - OnNodeLeft:   해당 노드의 파티션을 부하가 낮은 다른 노드로 migrate.
//   - 주기적 검사: split 임계치를 초과한 파티션을 자동으로 split.
//
// 메트릭 수집: PS가 Prometheus 메트릭을 HTTP로 노출하면 주기적으로 조회한다.
// (초기 구현: Prometheus scraping 방식. 임계치 기반 split 알고리즘은 추후 구체화)
type AutoRebalancePolicy struct {
	splitter     *rebalance.Splitter
	migrator     *rebalance.Migrator
	nodeRegistry cluster.NodeRegistry
	routingStore cluster.RoutingTableStore

	splitThreshold float64
	checkInterval  time.Duration
	opMu           *sync.Mutex // Server.opMu 공유 (직렬화)
}

func NewAutoRebalancePolicy(
	splitter *rebalance.Splitter,
	migrator *rebalance.Migrator,
	nodeRegistry cluster.NodeRegistry,
	routingStore cluster.RoutingTableStore,
	splitThreshold float64,
	checkInterval time.Duration,
	opMu *sync.Mutex,
) *AutoRebalancePolicy {
	return &AutoRebalancePolicy{
		splitter:       splitter,
		migrator:       migrator,
		nodeRegistry:   nodeRegistry,
		routingStore:   routingStore,
		splitThreshold: splitThreshold,
		checkInterval:  checkInterval,
		opMu:           opMu,
	}
}

// OnNodeJoined는 새 노드 합류 시 부하가 높은 파티션을 새 노드로 migrate한다.
// TODO: 메트릭 수집 방식 확정 후 실제 부하 기반 선택 로직 구현.
func (p *AutoRebalancePolicy) OnNodeJoined(_ context.Context, _ domain.NodeInfo) {}

// OnNodeLeft는 떠난 노드의 파티션을 다른 active 노드로 migrate한다.
func (p *AutoRebalancePolicy) OnNodeLeft(ctx context.Context, node domain.NodeInfo) {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	rt, err := p.routingStore.Load(ctx)
	if err != nil || rt == nil {
		return
	}

	nodes, err := p.nodeRegistry.ListNodes(ctx)
	if err != nil || len(nodes) == 0 {
		return
	}

	for _, entry := range rt.Entries() {
		if entry.Node.ID != node.ID {
			continue
		}
		target := pickLeastLoaded(nodes, node.ID)
		if target == "" {
			slog.Warn("auto rebalance: no available target node", "partition", entry.Partition.ID)
			continue
		}
		if err := p.migrator.Migrate(ctx, entry.Partition.ID, target); err != nil {
			slog.Error("auto rebalance: migrate failed", "partition", entry.Partition.ID, "target", target, "err", err)
		}
	}
}

// Start는 주기적 메트릭 검사 루프를 시작한다.
// AutoRebalancePolicy 사용 시 Server.Start 내에서 goroutine으로 호출한다.
func (p *AutoRebalancePolicy) Start(ctx context.Context) {
	ticker := time.NewTicker(p.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkAndSplit(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// checkAndSplit은 split 임계치를 초과한 파티션을 자동으로 split한다.
// TODO: Prometheus scraping으로 파티션별 RPS 조회 후 임계치 초과 시 split 구현.
func (p *AutoRebalancePolicy) checkAndSplit(_ context.Context) {}

// pickLeastLoaded는 excludeNodeID를 제외한 active 노드 중 부하가 낮은 노드 ID를 반환한다.
// 현재는 단순히 첫 번째 active 노드를 반환한다. (메트릭 기반 선택은 추후 구현)
func pickLeastLoaded(nodes []domain.NodeInfo, excludeNodeID string) string {
	for _, n := range nodes {
		if n.ID != excludeNodeID && n.Status == domain.NodeStatusActive {
			return n.ID
		}
	}
	return ""
}
