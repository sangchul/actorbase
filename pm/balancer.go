package pm

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/oomymy/actorbase/internal/cluster"
	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/rebalance"
	"github.com/oomymy/actorbase/internal/transport"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
	"github.com/oomymy/actorbase/pm/policy"
)

// autoBalancer는 주기적으로 클러스터 stats를 수집하고
// split/migrate 필요 여부를 판단하여 자동으로 실행한다.
type autoBalancer struct {
	cfg          *policy.ThresholdConfig
	splitter     *rebalance.Splitter
	migrator     *rebalance.Migrator
	nodeRegistry cluster.NodeRegistry
	routingStore cluster.RoutingTableStore
	connPool     *transport.ConnPool
	opMu         *sync.Mutex // pm.Server.opMu 공유

	mu                  sync.Mutex
	globalLastAction    time.Time
	partitionLastAction map[string]time.Time
}

func newAutoBalancer(
	cfg *policy.ThresholdConfig,
	splitter *rebalance.Splitter,
	migrator *rebalance.Migrator,
	nodeRegistry cluster.NodeRegistry,
	routingStore cluster.RoutingTableStore,
	connPool *transport.ConnPool,
	opMu *sync.Mutex,
) *autoBalancer {
	return &autoBalancer{
		cfg:                 cfg,
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
func (b *autoBalancer) start(ctx context.Context) {
	slog.Info("autoBalancer: started", "check_interval", b.cfg.CheckInterval)
	ticker := time.NewTicker(b.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.runOnce(ctx)
		case <-ctx.Done():
			slog.Info("autoBalancer: stopped")
			return
		}
	}
}

// nodeStats는 한 노드의 stats를 담는 내부 구조체.
type nodeStats struct {
	info       domain.NodeInfo
	resp       *pb.GetStatsResponse
	fetchError error
}

// runOnce는 한 번의 check 사이클을 실행한다.
func (b *autoBalancer) runOnce(ctx context.Context) {
	// 1. 라우팅 테이블 및 노드 목록 조회
	rt, err := b.routingStore.Load(ctx)
	if err != nil || rt == nil {
		slog.Warn("autoBalancer: failed to load routing table", "err", err)
		return
	}
	nodes, err := b.nodeRegistry.ListNodes(ctx)
	if err != nil || len(nodes) == 0 {
		slog.Warn("autoBalancer: failed to list nodes", "err", err)
		return
	}

	// 2. 모든 PS에 GetStats 병렬 호출
	stats := b.fetchAllStats(ctx, nodes)

	// 3. global cooldown 확인
	b.mu.Lock()
	globalCooldownActive := time.Since(b.globalLastAction) < b.cfg.Cooldown.Global
	b.mu.Unlock()
	if globalCooldownActive {
		slog.Debug("autoBalancer: global cooldown active, skipping")
		return
	}

	// 4. split 대상 탐색 및 실행
	b.checkSplit(ctx, rt, stats)

	// 5. balance 대상 탐색 및 실행
	b.checkBalance(ctx, rt, stats)
}

// fetchAllStats는 모든 노드에 GetStats를 병렬로 호출한다.
func (b *autoBalancer) fetchAllStats(ctx context.Context, nodes []domain.NodeInfo) []nodeStats {
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	results := make([]nodeStats, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, node domain.NodeInfo) {
			defer wg.Done()
			results[idx] = nodeStats{info: node}
			conn, connErr := b.connPool.Get(node.Address)
			if connErr != nil {
				results[idx].fetchError = connErr
				return
			}
			resp, statsErr := transport.NewPSControlClient(conn).GetStats(fetchCtx)
			results[idx].resp = resp
			results[idx].fetchError = statsErr
		}(i, n)
	}
	wg.Wait()
	return results
}

// checkSplit은 임계값을 초과한 파티션을 찾아 split을 실행한다.
func (b *autoBalancer) checkSplit(ctx context.Context, rt *domain.RoutingTable, stats []nodeStats) {
	for _, ns := range stats {
		if ns.fetchError != nil || ns.resp == nil {
			continue
		}
		for _, p := range ns.resp.Partitions {
			if b.isPartitionCooldown(p.PartitionId) {
				continue
			}
			needSplit := (b.cfg.Split.RPSThreshold > 0 && p.Rps > b.cfg.Split.RPSThreshold) ||
				(b.cfg.Split.KeyThreshold > 0 && p.KeyCount >= 0 && p.KeyCount > b.cfg.Split.KeyThreshold)
			if !needSplit {
				continue
			}

			// 라우팅 테이블에서 해당 파티션의 key range 조회
			entry, found := findEntry(rt, p.PartitionId)
			if !found {
				continue
			}
			splitKey := keyRangeMidpoint(entry.Partition.KeyRange.Start, entry.Partition.KeyRange.End)
			if splitKey == "" {
				slog.Warn("autoBalancer: cannot compute split key", "partition", p.PartitionId)
				continue
			}

			slog.Info("autoBalancer: split triggered",
				"partition", p.PartitionId, "actor_type", p.ActorType,
				"rps", p.Rps, "key_count", p.KeyCount, "split_key", splitKey)

			b.opMu.Lock()
			newID, splitErr := b.splitter.Split(ctx, p.ActorType, p.PartitionId, splitKey)
			b.opMu.Unlock()

			if splitErr != nil {
				slog.Error("autoBalancer: split failed",
					"partition", p.PartitionId, "err", splitErr)
				continue
			}
			slog.Info("autoBalancer: split done",
				"partition", p.PartitionId, "new_partition", newID)
			b.markAction(p.PartitionId)
			return // 한 사이클에 split 하나만 실행
		}
	}
}

// checkBalance는 노드 간 불균형을 찾아 migrate를 실행한다.
func (b *autoBalancer) checkBalance(ctx context.Context, rt *domain.RoutingTable, stats []nodeStats) {
	// 노드별 파티션 수 및 RPS 집계
	type nodeSummary struct {
		info           domain.NodeInfo
		partitionCount int
		totalRPS       float64
	}

	summaries := make([]nodeSummary, 0, len(stats))
	for _, ns := range stats {
		if ns.fetchError != nil || ns.resp == nil {
			continue
		}
		summaries = append(summaries, nodeSummary{
			info:           ns.info,
			partitionCount: int(ns.resp.PartitionCount),
			totalRPS:       ns.resp.NodeRps,
		})
	}
	if len(summaries) < 2 {
		return
	}

	// 가장 많은 노드와 가장 적은 노드 찾기 (파티션 수 기준)
	maxIdx, minIdx := 0, 0
	for i, s := range summaries {
		if s.partitionCount > summaries[maxIdx].partitionCount {
			maxIdx = i
		}
		if s.partitionCount < summaries[minIdx].partitionCount {
			minIdx = i
		}
	}
	maxNode := summaries[maxIdx]
	minNode := summaries[minIdx]

	// 파티션 수 불균형 확인
	partDiff := maxNode.partitionCount - minNode.partitionCount
	rpsImbalance := false
	if maxNode.totalRPS > 0 {
		imbalancePct := (maxNode.totalRPS - minNode.totalRPS) / maxNode.totalRPS * 100
		rpsImbalance = imbalancePct > b.cfg.Balance.RPSImbalancePct
	}

	if partDiff <= b.cfg.Balance.MaxPartitionDiff && !rpsImbalance {
		return
	}

	// maxNode에서 파티션 하나 선택 (쿨다운 중이 아닌 것)
	partitionID, actorType := pickMigratable(rt, maxNode.info.ID, b.isPartitionCooldownFunc())
	if partitionID == "" {
		slog.Debug("autoBalancer: no migratable partition found on overloaded node",
			"node", maxNode.info.ID)
		return
	}

	slog.Info("autoBalancer: balance triggered",
		"from", maxNode.info.ID, "to", minNode.info.ID,
		"partition", partitionID, "part_diff", partDiff)

	b.opMu.Lock()
	migrateErr := b.migrator.Migrate(ctx, actorType, partitionID, minNode.info.ID)
	b.opMu.Unlock()

	if migrateErr != nil {
		slog.Error("autoBalancer: migrate failed",
			"partition", partitionID, "err", migrateErr)
		return
	}
	slog.Info("autoBalancer: migrate done",
		"partition", partitionID, "from", maxNode.info.ID, "to", minNode.info.ID)
	b.markAction(partitionID)
}

// markAction은 global 및 partition 쿨다운 타이머를 갱신한다.
func (b *autoBalancer) markAction(partitionID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.globalLastAction = now
	b.partitionLastAction[partitionID] = now
}

// isPartitionCooldown은 파티션이 쿨다운 중인지 확인한다.
func (b *autoBalancer) isPartitionCooldown(partitionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.partitionLastAction[partitionID]
	return ok && time.Since(t) < b.cfg.Cooldown.Partition
}

// isPartitionCooldownFunc는 isPartitionCooldown을 함수 값으로 반환한다.
func (b *autoBalancer) isPartitionCooldownFunc() func(string) bool {
	return b.isPartitionCooldown
}

// ── 헬퍼 ─────────────────────────────────────────────────────────────────────

// findEntry는 라우팅 테이블에서 partitionID에 해당하는 엔트리를 찾는다.
func findEntry(rt *domain.RoutingTable, partitionID string) (domain.RouteEntry, bool) {
	for _, e := range rt.Entries() {
		if e.Partition.ID == partitionID {
			return e, true
		}
	}
	return domain.RouteEntry{}, false
}

// pickMigratable은 nodeID의 파티션 중 쿨다운이 아닌 것을 하나 반환한다.
func pickMigratable(rt *domain.RoutingTable, nodeID string, cooldownFn func(string) bool) (partitionID, actorType string) {
	for _, e := range rt.Entries() {
		if e.Node.ID == nodeID && !cooldownFn(e.Partition.ID) {
			return e.Partition.ID, e.Partition.ActorType
		}
	}
	return "", ""
}

// keyRangeMidpoint는 [start, end) 키 범위의 중간값을 계산한다.
// 두 문자열의 바이트 레벨 산술 평균을 사용한다.
func keyRangeMidpoint(start, end string) string {
	if start == "" && end == "" {
		return "m" // 전체 범위의 임의 중간값
	}
	if end == "" {
		// [start, ∞) 범위: start 뒤에 중간 값 문자 추가
		return start + "m"
	}

	// 두 문자열의 바이트 레벨 중간값 계산
	sb := []byte(start)
	eb := []byte(end)

	// 짧은 쪽을 null로 패딩
	maxLen := len(eb)
	if len(sb) > maxLen {
		maxLen = len(sb)
	}
	for len(sb) < maxLen {
		sb = append(sb, 0)
	}
	for len(eb) < maxLen {
		eb = append(eb, 0)
	}

	result := make([]byte, maxLen)
	carry := 0
	for i := maxLen - 1; i >= 0; i-- {
		sum := int(sb[i]) + int(eb[i]) + carry*256
		result[i] = byte(sum / 2)
		carry = sum % 2
	}

	// 후행 null 제거
	n := len(result)
	for n > 0 && result[n-1] == 0 {
		n--
	}
	if n == 0 {
		n = 1
	}
	mid := string(result[:n])

	// midpoint가 start와 같으면 start에 문자 추가
	if mid <= start {
		return start + "m"
	}
	return mid
}
