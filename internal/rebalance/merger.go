package rebalance

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
)

// Merger는 인접한 두 파티션의 merge를 조율한다.
// lower 파티션이 upper 파티션의 상태를 흡수한다.
type Merger struct {
	routingStore cluster.RoutingTableStore
	connPool     *transport.ConnPool
}

// NewMerger는 Merger를 생성한다.
func NewMerger(routingStore cluster.RoutingTableStore, connPool *transport.ConnPool) *Merger {
	return &Merger{
		routingStore: routingStore,
		connPool:     connPool,
	}
}

// Merge는 lowerPartitionID에 upperPartitionID를 흡수시킨다.
// 두 파티션은 같은 actorType, 인접 KeyRange, 같은 노드에 있어야 한다.
func (m *Merger) Merge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error {
	// 1. 현재 라우팅 테이블 조회
	rt, err := m.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("load routing table: %w", err)
	}
	if rt == nil {
		return fmt.Errorf("routing table is empty")
	}

	// 2. 두 파티션 존재 검증
	lowerEntry, ok := rt.LookupByPartition(lowerPartitionID)
	if !ok {
		return fmt.Errorf("lower partition %s not found in routing table", lowerPartitionID)
	}
	upperEntry, ok := rt.LookupByPartition(upperPartitionID)
	if !ok {
		return fmt.Errorf("upper partition %s not found in routing table", upperPartitionID)
	}

	// 3. actor type 검증
	if lowerEntry.Partition.ActorType != actorType {
		return fmt.Errorf("actor type mismatch: lower partition %s has actor type %q, got %q",
			lowerPartitionID, lowerEntry.Partition.ActorType, actorType)
	}
	if upperEntry.Partition.ActorType != actorType {
		return fmt.Errorf("actor type mismatch: upper partition %s has actor type %q, got %q",
			upperPartitionID, upperEntry.Partition.ActorType, actorType)
	}

	// 4. 인접 KeyRange 검증: lower.End == upper.Start
	if lowerEntry.Partition.KeyRange.End != upperEntry.Partition.KeyRange.Start {
		return fmt.Errorf("partitions are not adjacent: lower.End=%q != upper.Start=%q",
			lowerEntry.Partition.KeyRange.End, upperEntry.Partition.KeyRange.Start)
	}

	// 5. 같은 노드 검증
	if lowerEntry.Node.ID != upperEntry.Node.ID {
		return fmt.Errorf("partitions are on different nodes: lower=%s, upper=%s",
			lowerEntry.Node.ID, upperEntry.Node.ID)
	}

	// 6. 두 파티션 모두 Active 상태 검증
	if lowerEntry.PartitionStatus != domain.PartitionStatusActive {
		return fmt.Errorf("lower partition %s is not active", lowerPartitionID)
	}
	if upperEntry.PartitionStatus != domain.PartitionStatusActive {
		return fmt.Errorf("upper partition %s is not active", upperPartitionID)
	}

	slog.Info("rebalance: merge starting",
		"actor_type", actorType,
		"lower", lowerPartitionID, "upper", upperPartitionID,
		"node", lowerEntry.Node.ID)

	// 7. 두 파티션 Draining (라우팅 테이블 갱신 → SDK: ErrPartitionBusy)
	drainingRT, err := buildMergeDrainingTable(rt, lowerPartitionID, upperPartitionID)
	if err != nil {
		return fmt.Errorf("build draining routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, drainingRT); err != nil {
		return fmt.Errorf("save draining routing table: %w", err)
	}

	// 8. PS에 ExecuteMerge RPC
	conn, err := m.connPool.Get(lowerEntry.Node.Address)
	if err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to PS %s: %w", lowerEntry.Node.Address, err)
	}
	psCtrl := transport.NewPSControlClient(conn)
	if err := psCtrl.ExecuteMerge(ctx, actorType, lowerPartitionID, upperPartitionID); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("execute merge on PS: %w", err)
	}

	// 9. 라우팅 테이블 갱신: lower의 KeyRange.End = upper.KeyRange.End, upper 삭제
	mergedRT, err := buildMergedTable(rt, lowerPartitionID, upperPartitionID, upperEntry.Partition.KeyRange.End)
	if err != nil {
		return fmt.Errorf("build merged routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, mergedRT); err != nil {
		return fmt.Errorf("save merged routing table: %w", err)
	}

	slog.Info("rebalance: merge complete",
		"actor_type", actorType,
		"lower", lowerPartitionID, "upper", upperPartitionID)

	return nil
}

// revertToActive는 merge 실패 시 라우팅 테이블을 이전 상태로 복구한다.
func (m *Merger) revertToActive(ctx context.Context, original *domain.RoutingTable) {
	if err := m.routingStore.Save(ctx, original); err != nil {
		slog.Error("failed to revert routing table after merge failure", "err", err)
	}
}

// buildMergeDrainingTable은 두 파티션의 PartitionStatus를 Draining으로 변경한 새 RoutingTable을 반환한다.
func buildMergeDrainingTable(rt *domain.RoutingTable, lowerID, upperID string) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	for i, e := range entries {
		if e.Partition.ID == lowerID || e.Partition.ID == upperID {
			entries[i].PartitionStatus = domain.PartitionStatusDraining
		}
	}
	return domain.NewRoutingTable(rt.Version()+1, entries)
}

// buildMergedTable은 lower 파티션의 KeyRange를 확장하고 upper 파티션을 삭제한 새 RoutingTable을 반환한다.
func buildMergedTable(rt *domain.RoutingTable, lowerID, upperID, newEnd string) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	result := make([]domain.RouteEntry, 0, len(entries)-1)
	for _, e := range entries {
		if e.Partition.ID == upperID {
			continue // upper 파티션 삭제
		}
		if e.Partition.ID == lowerID {
			e.Partition.KeyRange.End = newEnd
			e.PartitionStatus = domain.PartitionStatusActive
		}
		result = append(result, e)
	}
	return domain.NewRoutingTable(rt.Version()+1, result)
}
