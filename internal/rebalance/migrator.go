package rebalance

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
)

// Migrator orchestrates partition migrations.
type Migrator struct {
	routingStore cluster.RoutingTableStore
	nodeRegistry cluster.NodeRegistry
	connPool     *transport.ConnPool
}

// NewMigrator creates a Migrator.
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

// Migrate moves partitionID to targetNodeID.
// actorType must match the actual actor type of partitionID; an error is
// returned on mismatch.
func (m *Migrator) Migrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
	// 1. Load and validate the routing table.
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
	if entry.Partition.ActorType != actorType {
		return fmt.Errorf("actor type mismatch: partition %s has actor type %q, got %q",
			partitionID, entry.Partition.ActorType, actorType)
	}
	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return fmt.Errorf("partition %s is already draining", partitionID)
	}
	if entry.Node.ID == targetNodeID {
		return fmt.Errorf("partition %s is already on node %s", partitionID, targetNodeID)
	}

	// 2. Validate the target node.
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

	// 3. Update the partition to Draining in the routing table and persist to etcd.
	drainingRT, err := buildDrainingTable(rt, partitionID)
	if err != nil {
		return fmt.Errorf("build draining routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, drainingRT); err != nil {
		return fmt.Errorf("save draining routing table: %w", err)
	}

	// 4. Send ExecuteMigrateOut to the source PS.
	sourceConn, err := m.connPool.Get(entry.Node.Address)
	if err != nil {
		// Revert the routing table on failure.
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to source PS %s: %w", entry.Node.Address, err)
	}
	sourceCtrl := transport.NewPSControlClient(sourceConn)
	if err := sourceCtrl.ExecuteMigrateOut(ctx, entry.Partition.ActorType, partitionID, targetNodeID, targetNode.Address); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("execute migrate out: %w", err)
	}

	// 5. Send PreparePartition to the target PS (with retry).
	targetConn, err := m.connPool.Get(targetNode.Address)
	if err != nil {
		// Source has already been evicted — cannot recover. Revert routing to source only.
		slog.Error("connect to target PS failed after migrate out", "target", targetNode.Address, "err", err)
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to target PS %s: %w", targetNode.Address, err)
	}
	targetCtrl := transport.NewPSControlClient(targetConn)
	kr := entry.Partition.KeyRange
	if err := targetCtrl.PreparePartition(ctx, entry.Partition.ActorType, partitionID, kr.Start, kr.End); err != nil {
		slog.Error("prepare partition failed, reverting routing", "partition", partitionID, "err", err)
		m.revertToActive(ctx, rt)
		return fmt.Errorf("prepare partition on target PS: %w", err)
	}

	// 6. Update the routing table: move the partition to targetNode (Active).
	finalRT, err := buildMigratedTable(rt, partitionID, targetNode)
	if err != nil {
		return fmt.Errorf("build migrated routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, finalRT); err != nil {
		return fmt.Errorf("save final routing table: %w", err)
	}

	return nil
}

// revertToActive restores the routing table to the previous Active state on
// migration failure. Restoration errors are only logged and otherwise ignored.
func (m *Migrator) revertToActive(ctx context.Context, original *domain.RoutingTable) {
	if err := m.routingStore.Save(ctx, original); err != nil {
		slog.Error("failed to revert routing table", "err", err)
	}
}

// buildDrainingTable returns a new RoutingTable with the PartitionStatus of
// partitionID set to Draining.
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

// buildMigratedTable returns a new RoutingTable with partitionID's node
// updated to targetNode and its status set to Active.
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

// Failover moves partitionID from an unreachable source PS to targetNodeID.
// It skips ExecuteMigrateOut and restores state on the target PS via the last
// checkpoint plus WAL replay. No data loss occurs when using a networked
// WALStore (e.g. Redis Streams). With an fs-based WALStore, WAL entries
// written after the last checkpoint are lost.
func (m *Migrator) Failover(ctx context.Context, partitionID, targetNodeID string) error {
	// 1. Load and validate the routing table.
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

	// 2. Validate the target node.
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

	// 3. Set routing to Draining (SDK receives ErrPartitionBusy and retries).
	drainingRT, err := buildDrainingTable(rt, partitionID)
	if err != nil {
		return fmt.Errorf("build draining routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, drainingRT); err != nil {
		return fmt.Errorf("save draining routing table: %w", err)
	}

	// 4. [ExecuteMigrateOut skipped — source PS is dead]

	// 5. Send PreparePartition to the target PS (restores via checkpoint + WAL replay).
	targetConn, err := m.connPool.Get(targetNode.Address)
	if err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to target PS %s: %w", targetNode.Address, err)
	}
	targetCtrl := transport.NewPSControlClient(targetConn)
	kr := entry.Partition.KeyRange
	if err := targetCtrl.PreparePartition(ctx, entry.Partition.ActorType, partitionID, kr.Start, kr.End); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("prepare partition on target PS: %w", err)
	}

	// 6. Set routing to targetNode Active.
	finalRT, err := buildMigratedTable(rt, partitionID, targetNode)
	if err != nil {
		return fmt.Errorf("build migrated routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, finalRT); err != nil {
		return fmt.Errorf("save final routing table: %w", err)
	}

	return nil
}

func findNode(nodes []domain.NodeInfo, nodeID string) (domain.NodeInfo, bool) {
	for _, n := range nodes {
		if n.ID == nodeID {
			return n, true
		}
	}
	return domain.NodeInfo{}, false
}
