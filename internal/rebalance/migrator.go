package rebalance

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/oomymy/actorbase/internal/cluster"
	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/transport"
)

// Migrator는 파티션 migration을 조율한다.
type Migrator struct {
	routingStore cluster.RoutingTableStore
	nodeRegistry cluster.NodeRegistry
	connPool     *transport.ConnPool
}

// NewMigrator는 Migrator를 생성한다.
func NewMigrator(
	routingStore cluster.RoutingTableStore,
	nodeRegistry cluster.NodeRegistry,
	connPool *transport.ConnPool,
) *Migrator {
	return &Migrator{
		routingStore: routingStore,
		nodeRegistry: nodeRegistry,
		connPool:     connPool,
	}
}

// Migrate는 partitionID를 targetNodeID로 이동시킨다.
func (m *Migrator) Migrate(ctx context.Context, partitionID, targetNodeID string) error {
	// 1. 라우팅 테이블 조회 및 검증
	rt, err := m.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("load routing table: %w", err)
	}
	if rt == nil {
		return fmt.Errorf("routing table is empty")
	}

	entry, ok := rt.LookupByPartition(partitionID)
	if !ok {
		return fmt.Errorf("partition %s not found", partitionID)
	}
	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return fmt.Errorf("partition %s is already draining", partitionID)
	}
	if entry.Node.ID == targetNodeID {
		return fmt.Errorf("partition %s is already on node %s", partitionID, targetNodeID)
	}

	// 2. target 노드 검증
	nodes, err := m.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	targetNode, found := findNode(nodes, targetNodeID)
	if !found {
		return fmt.Errorf("target node %s not found", targetNodeID)
	}
	if targetNode.Status != domain.NodeStatusActive {
		return fmt.Errorf("target node %s is not active", targetNodeID)
	}

	// 3. 라우팅 테이블에서 파티션을 Draining으로 갱신 → etcd 저장
	drainingRT, err := buildDrainingTable(rt, partitionID)
	if err != nil {
		return fmt.Errorf("build draining routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, drainingRT); err != nil {
		return fmt.Errorf("save draining routing table: %w", err)
	}

	// 4. Source PS에 ExecuteMigrateOut 명령
	sourceConn, err := m.connPool.Get(entry.Node.Address)
	if err != nil {
		// 실패 시 라우팅 테이블 복구
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to source PS %s: %w", entry.Node.Address, err)
	}
	sourceCtrl := transport.NewPSControlClient(sourceConn)
	if err := sourceCtrl.ExecuteMigrateOut(ctx, partitionID, targetNodeID, targetNode.Address); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("execute migrate out: %w", err)
	}

	// 5. Target PS에 PreparePartition 명령 (재시도 포함)
	targetConn, err := m.connPool.Get(targetNode.Address)
	if err != nil {
		// source는 이미 evict됨 → 복구 불가. 라우팅만 source로 되돌림.
		slog.Error("connect to target PS failed after migrate out", "target", targetNode.Address, "err", err)
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to target PS %s: %w", targetNode.Address, err)
	}
	targetCtrl := transport.NewPSControlClient(targetConn)
	kr := entry.Partition.KeyRange
	if err := targetCtrl.PreparePartition(ctx, partitionID, kr.Start, kr.End); err != nil {
		slog.Error("prepare partition failed, reverting routing", "partition", partitionID, "err", err)
		m.revertToActive(ctx, rt)
		return fmt.Errorf("prepare partition on target PS: %w", err)
	}

	// 6. 라우팅 테이블 갱신: 파티션 → targetNode (Active)
	finalRT, err := buildMigratedTable(rt, partitionID, targetNode)
	if err != nil {
		return fmt.Errorf("build migrated routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, finalRT); err != nil {
		return fmt.Errorf("save final routing table: %w", err)
	}

	return nil
}

// revertToActive는 migration 실패 시 라우팅 테이블을 이전 Active 상태로 복구한다.
// 복구 실패는 로그만 남기고 무시한다.
func (m *Migrator) revertToActive(ctx context.Context, original *domain.RoutingTable) {
	if err := m.routingStore.Save(ctx, original); err != nil {
		slog.Error("failed to revert routing table", "err", err)
	}
}

// buildDrainingTable은 partitionID의 PartitionStatus를 Draining으로 변경한 새 RoutingTable을 반환한다.
func buildDrainingTable(rt *domain.RoutingTable, partitionID string) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	for i, e := range entries {
		if e.Partition.ID == partitionID {
			entries[i].PartitionStatus = domain.PartitionStatusDraining
			break
		}
	}
	return domain.NewRoutingTable(rt.Version()+1, entries)
}

// buildMigratedTable은 partitionID의 노드를 targetNode(Active)로 변경한 새 RoutingTable을 반환한다.
func buildMigratedTable(rt *domain.RoutingTable, partitionID string, targetNode domain.NodeInfo) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	for i, e := range entries {
		if e.Partition.ID == partitionID {
			entries[i].Node = targetNode
			entries[i].PartitionStatus = domain.PartitionStatusActive
			break
		}
	}
	return domain.NewRoutingTable(rt.Version()+1, entries)
}

func findNode(nodes []domain.NodeInfo, nodeID string) (domain.NodeInfo, bool) {
	for _, n := range nodes {
		if n.ID == nodeID {
			return n, true
		}
	}
	return domain.NodeInfo{}, false
}
