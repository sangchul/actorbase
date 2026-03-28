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

// Merger orchestrates the merge of two adjacent partitions.
// The lower partition absorbs the state of the upper partition.
type Merger struct {
	routingStore cluster.RoutingTableStore
	psFactory    transport.PSClientFactory
}

// NewMerger creates a Merger.
func NewMerger(routingStore cluster.RoutingTableStore, psFactory transport.PSClientFactory) *Merger {
	return &Merger{
		routingStore: routingStore,
		psFactory:    psFactory,
	}
}

// Merge absorbs upperPartitionID into lowerPartitionID.
// Both partitions must share the same actorType, have adjacent KeyRanges,
// and reside on the same node.
func (m *Merger) Merge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error {
	// 1. Load the current routing table.
	rt, err := m.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("load routing table: %w", err)
	}
	if rt == nil {
		return fmt.Errorf("routing table is empty")
	}

	// 2. Validate that both partitions exist.
	lowerEntry, ok := rt.LookupByPartition(lowerPartitionID)
	if !ok {
		return fmt.Errorf("lower partition %s not found in routing table", lowerPartitionID)
	}
	upperEntry, ok := rt.LookupByPartition(upperPartitionID)
	if !ok {
		return fmt.Errorf("upper partition %s not found in routing table", upperPartitionID)
	}

	// 3. Validate actor type.
	if lowerEntry.Partition.ActorType != actorType {
		return fmt.Errorf("actor type mismatch: lower partition %s has actor type %q, got %q",
			lowerPartitionID, lowerEntry.Partition.ActorType, actorType)
	}
	if upperEntry.Partition.ActorType != actorType {
		return fmt.Errorf("actor type mismatch: upper partition %s has actor type %q, got %q",
			upperPartitionID, upperEntry.Partition.ActorType, actorType)
	}

	// 4. Validate adjacency: lower.End == upper.Start.
	if lowerEntry.Partition.KeyRange.End != upperEntry.Partition.KeyRange.Start {
		return fmt.Errorf("partitions are not adjacent: lower.End=%q != upper.Start=%q",
			lowerEntry.Partition.KeyRange.End, upperEntry.Partition.KeyRange.Start)
	}

	// 5. Validate that both partitions are on the same node.
	if lowerEntry.Node.ID != upperEntry.Node.ID {
		return fmt.Errorf("partitions are on different nodes: lower=%s, upper=%s",
			lowerEntry.Node.ID, upperEntry.Node.ID)
	}

	// 6. Validate that both partitions are in Active status.
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

	// 7. Set both partitions to Draining in the routing table (SDK receives ErrPartitionBusy).
	drainingRT, err := buildMergeDrainingTable(rt, lowerPartitionID, upperPartitionID)
	if err != nil {
		return fmt.Errorf("build draining routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, drainingRT); err != nil {
		return fmt.Errorf("save draining routing table: %w", err)
	}

	// 8. Send ExecuteMerge RPC to the PS.
	psCtrl, err := m.psFactory.GetClient(lowerEntry.Node.Address)
	if err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("connect to PS %s: %w", lowerEntry.Node.Address, err)
	}
	if err := psCtrl.ExecuteMerge(ctx, actorType, lowerPartitionID, upperPartitionID); err != nil {
		m.revertToActive(ctx, rt)
		return fmt.Errorf("execute merge on PS: %w", err)
	}

	// 9. Update routing table: extend lower's KeyRange.End to upper's KeyRange.End and remove upper.
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

// ResumeMerge is called by a newly elected PM leader when the routing table
// shows both lower and upper partitions stuck in Draining state due to a PM crash.
//
// It does not check PartitionStatusDraining and re-runs the remaining steps:
// 1. ExecuteMerge on PS — if the upper partition is already gone (already merged),
//    the ErrNotFound response is treated as "already done".
// 2. Final routing table update: lower range extended, upper removed.
func (m *Merger) ResumeMerge(ctx context.Context, actorType, lowerID, upperID string) error {
	rt, err := m.routingStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("load routing table: %w", err)
	}
	if rt == nil {
		return fmt.Errorf("routing table is empty")
	}

	lowerEntry, lowerOK := rt.LookupByPartition(lowerID)
	upperEntry, upperOK := rt.LookupByPartition(upperID)
	if !lowerOK {
		return fmt.Errorf("lower partition %s not found", lowerID)
	}
	if !upperOK {
		// Upper already gone from routing — merge completed.
		return nil
	}

	psCtrl, err := m.psFactory.GetClient(lowerEntry.Node.Address)
	if err != nil {
		return fmt.Errorf("connect to PS %s: %w", lowerEntry.Node.Address, err)
	}

	// Step 1: ExecuteMerge — treat ErrNotFound for upper as already merged.
	if mergeErr := psCtrl.ExecuteMerge(ctx, actorType, lowerID, upperID); mergeErr != nil {
		if !errors.Is(mergeErr, provider.ErrNotFound) {
			return fmt.Errorf("resume execute merge: %w", mergeErr)
		}
		slog.Info("rebalance: ResumeMerge: upper partition already merged on PS", "upper", upperID)
	}

	// Step 2: Update routing: extend lower range to cover upper range, remove upper.
	mergedRT, err := buildMergedTable(rt, lowerID, upperID, upperEntry.Partition.KeyRange.End)
	if err != nil {
		return fmt.Errorf("build merged routing table: %w", err)
	}
	if err := m.routingStore.Save(ctx, mergedRT); err != nil {
		return fmt.Errorf("save merged routing table: %w", err)
	}
	return nil
}

// revertToActive restores the routing table to the previous state on merge failure.
func (m *Merger) revertToActive(ctx context.Context, original *domain.RoutingTable) {
	if err := m.routingStore.Save(ctx, original); err != nil {
		slog.Error("failed to revert routing table after merge failure", "err", err)
	}
}

// buildMergeDrainingTable returns a new RoutingTable with both partitions'
// PartitionStatus set to Draining.
func buildMergeDrainingTable(rt *domain.RoutingTable, lowerID, upperID string) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	for i, e := range entries {
		if e.Partition.ID == lowerID || e.Partition.ID == upperID {
			entries[i].PartitionStatus = domain.PartitionStatusDraining
		}
	}
	return domain.NewRoutingTable(rt.Version()+1, entries)
}

// buildMergedTable returns a new RoutingTable with the lower partition's
// KeyRange extended and the upper partition removed.
func buildMergedTable(rt *domain.RoutingTable, lowerID, upperID, newEnd string) (*domain.RoutingTable, error) {
	entries := rt.Entries()
	result := make([]domain.RouteEntry, 0, len(entries)-1)
	for _, e := range entries {
		if e.Partition.ID == upperID {
			continue // Remove the upper partition.
		}
		if e.Partition.ID == lowerID {
			e.Partition.KeyRange.End = newEnd
			e.PartitionStatus = domain.PartitionStatusActive
		}
		result = append(result, e)
	}
	return domain.NewRoutingTable(rt.Version()+1, result)
}
