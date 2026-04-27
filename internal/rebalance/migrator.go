package rebalance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	"github.com/sangchul/actorbase/provider"
)

// Migrator orchestrates partition migrations.
type Migrator struct {
	routingStore cluster.RoutingTableStore
	nodeCatalog  cluster.NodeCatalog
	psFactory    transport.PSClientFactory
}

// NewMigrator creates a Migrator.
func NewMigrator(
	routingStore cluster.RoutingTableStore,
	nodeCatalog cluster.NodeCatalog,
	psFactory transport.PSClientFactory,
) *Migrator {
	return &Migrator{
		routingStore: routingStore,
		nodeCatalog:  nodeCatalog,
		psFactory:    psFactory,
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
	if entry.NodeID == targetNodeID {
		return fmt.Errorf("partition %s is already on node %s", partitionID, targetNodeID)
	}

	// 2. Validate the target node.
	nodes, err := m.nodeCatalog.ListNodes(ctx)
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

	// Leadership checkpoint: abort if context was cancelled between steps.
	if err := ctx.Err(); err != nil {
		m.revertToActive(context.Background(), rt) //nolint:contextcheck — ctx is already done
		return fmt.Errorf("leadership lost before migrate out: %w", err)
	}

	// 4. Send ExecuteMigrateOut to the source PS.
	sourceAddr, ok := rt.NodeAddress(entry.NodeID)
	if !ok {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("no address known for source node %s", entry.NodeID)
	}
	sourceCtrl, err := m.psFactory.GetClient(sourceAddr)
	if err != nil {
		// Revert the routing table on failure.
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to source PS %s: %w", sourceAddr, err)
	}
	if err := sourceCtrl.ExecuteMigrateOut(ctx, entry.Partition.ActorType, partitionID, targetNodeID, targetNode.Address, entry.Epoch); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("execute migrate out: %w", err)
	}

	// Leadership checkpoint: abort if context was cancelled between steps.
	if err := ctx.Err(); err != nil {
		// Source has already evicted the partition; revert routing so the next PM
		// leader can assign it. Use background context since leaderCtx is done.
		m.revertToActive(context.Background(), rt) //nolint:contextcheck — ctx is already done
		return fmt.Errorf("leadership lost after migrate out, before prepare: %w", err)
	}

	// 5. Send PreparePartition to the target PS (with retry).
	targetCtrl, err := m.psFactory.GetClient(targetNode.Address)
	if err != nil {
		// Source has already been evicted — cannot recover. Revert routing to source only.
		slog.Error("connect to target PS failed after migrate out", "target", targetNode.Address, "err", err)
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to target PS %s: %w", targetNode.Address, err)
	}
	kr := entry.Partition.KeyRange
	nextEpoch := uint64(rt.Version() + 1)
	if err := targetCtrl.PreparePartition(ctx, entry.Partition.ActorType, partitionID, kr.Start, kr.End, nextEpoch); err != nil {
		slog.Error("prepare partition failed, reverting routing", "partition", partitionID, "err", err)
		m.revertToActive(ctx, rt)
		return fmt.Errorf("prepare partition on target PS: %w", err)
	}

	// 6. Update the routing table: move the partition to targetNode (Active).
	finalRT, err := buildMigratedTable(rt, partitionID, targetNode.ID, targetNode.Address, nextEpoch)
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
	newRT, err := domain.NewRoutingTable(rt.Version()+1, entries)
	if err != nil {
		return nil, err
	}
	return newRT.WithNodeAddrs(rt.NodeAddrs()), nil
}

// buildMigratedTable returns a new RoutingTable with partitionID's NodeID
// updated to targetNodeID and its status set to Active.
// The target node's address is added to the nodeAddrs map.
func buildMigratedTable(rt *domain.RoutingTable, partitionID, targetNodeID, targetAddr string, epoch uint64) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	for i, e := range entries {
		if e.Partition.ID == partitionID {
			entries[i].NodeID = targetNodeID
			entries[i].PartitionStatus = domain.PartitionStatusActive
			entries[i].Epoch = epoch
			break
		}
	}
	newRT, err := domain.NewRoutingTable(rt.Version()+1, entries)
	if err != nil {
		return nil, err
	}
	// Copy existing addresses and add/update the target node's address.
	newAddrs := make(map[string]string, len(rt.NodeAddrs())+1)
	for k, v := range rt.NodeAddrs() {
		newAddrs[k] = v
	}
	newAddrs[targetNodeID] = targetAddr
	return newRT.WithNodeAddrs(newAddrs), nil
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
	nodes, err := m.nodeCatalog.ListNodes(ctx)
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
	targetCtrl, err := m.psFactory.GetClient(targetNode.Address)
	if err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to target PS %s: %w", targetNode.Address, err)
	}
	kr := entry.Partition.KeyRange
	nextEpoch := uint64(rt.Version() + 1)
	if err := targetCtrl.PreparePartition(ctx, entry.Partition.ActorType, partitionID, kr.Start, kr.End, nextEpoch); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("prepare partition on target PS: %w", err)
	}

	// 6. Set routing to targetNode Active.
	finalRT, err := buildMigratedTable(rt, partitionID, targetNode.ID, targetNode.Address, nextEpoch)
	if err != nil {
		return fmt.Errorf("build migrated routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, finalRT); err != nil {
		return fmt.Errorf("save final routing table: %w", err)
	}

	return nil
}

// ResumeMigrate is called by a newly elected PM leader when the routing table
// shows a partition stuck in Draining state due to a previous PM crash.
//
// It does not check PartitionStatusDraining and re-runs all remaining steps:
// 1. ExecuteMigrateOut on source PS — source may have already evicted the partition;
//    errors indicating "not found" or "not owned" are treated as already-done.
// 2. PreparePartition on target PS.
// 3. Final routing table update to Active on targetNode.
func (m *Migrator) ResumeMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
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

	nodes, err := m.nodeCatalog.ListNodes(ctx)
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

	// Step 1: ExecuteMigrateOut on source PS.
	// The source may have already evicted the partition (if PM crashed after step 4).
	// ErrNotFound / ErrPartitionNotOwned from source → already evicted → continue.
	sourceAddr, _ := rt.NodeAddress(entry.NodeID)
	sourceCtrl, err := m.psFactory.GetClient(sourceAddr)
	if err != nil || sourceAddr == "" {
		slog.Warn("rebalance: ResumeMigrate: cannot connect to source, assuming already evicted",
			"source", sourceAddr, "err", err)
	} else {
		if evictErr := sourceCtrl.ExecuteMigrateOut(ctx, actorType, partitionID, targetNodeID, targetNode.Address, entry.Epoch); evictErr != nil {
			if isGoneErr(evictErr) {
				slog.Info("rebalance: ResumeMigrate: source already evicted partition", "partition", partitionID)
			} else {
				return fmt.Errorf("resume execute migrate out: %w", evictErr)
			}
		}
	}

	// Step 2: PreparePartition on target PS.
	targetCtrl, err := m.psFactory.GetClient(targetNode.Address)
	if err != nil {
		return fmt.Errorf("connect to target PS %s: %w", targetNode.Address, err)
	}
	kr := entry.Partition.KeyRange
	nextEpoch := uint64(rt.Version() + 1)
	if err := targetCtrl.PreparePartition(ctx, actorType, partitionID, kr.Start, kr.End, nextEpoch); err != nil {
		return fmt.Errorf("resume prepare partition: %w", err)
	}

	// Step 3: Update routing table to Active on target.
	finalRT, err := buildMigratedTable(rt, partitionID, targetNode.ID, targetNode.Address, nextEpoch)
	if err != nil {
		return fmt.Errorf("build migrated routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, finalRT); err != nil {
		return fmt.Errorf("save final routing table: %w", err)
	}
	return nil
}

// isGoneErr returns true when the error indicates the partition is no longer
// present on the PS (already evicted or not found). Used during resume to
// treat "already done" steps as success.
func isGoneErr(err error) bool {
	return errors.Is(err, provider.ErrNotFound) || errors.Is(err, provider.ErrPartitionNotOwned)
}

func findNode(nodes []domain.NodeInfo, nodeID string) (domain.NodeInfo, bool) {
	for _, n := range nodes {
		if n.ID == nodeID {
			return n, true
		}
	}
	return domain.NodeInfo{}, false
}
