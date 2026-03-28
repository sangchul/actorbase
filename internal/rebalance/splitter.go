package rebalance

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
)

// Splitter orchestrates partition splits.
type Splitter struct {
	routingStore cluster.RoutingTableStore
	psFactory    transport.PSClientFactory
}

// NewSplitter creates a Splitter.
func NewSplitter(routingStore cluster.RoutingTableStore, psFactory transport.PSClientFactory) *Splitter {
	return &Splitter{
		routingStore: routingStore,
		psFactory:    psFactory,
	}
}

// Split divides partitionID into two partitions at splitKey.
// actorType must match the actual actor type of partitionID; an error is
// returned on mismatch. Returns the ID of the newly created partition
// (the upper half). Both partitions remain on the same node after the split.
// newPartitionID is optional: if non-empty, it is used as the upper-half ID (for
// idempotent resume after a PM crash). If empty, a new UUID is generated.
func (s *Splitter) Split(ctx context.Context, actorType, partitionID, splitKey, newPartitionID string) (string, error) {
	// 1. Load the current routing table.
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		return "", fmt.Errorf("load routing table: %w", err)
	}
	if rt == nil {
		return "", fmt.Errorf("routing table is empty")
	}

	// 2. Validate that the partition exists.
	entry, ok := rt.LookupByPartition(partitionID)
	if !ok {
		return "", fmt.Errorf("partition %s not found in routing table", partitionID)
	}

	// 3. Validate actor type.
	if entry.Partition.ActorType != actorType {
		return "", fmt.Errorf("actor type mismatch: partition %s has actor type %q, got %q",
			partitionID, entry.Partition.ActorType, actorType)
	}

	// 4. Validate splitKey (only when explicitly provided).
	kr := entry.Partition.KeyRange
	if splitKey != "" {
		if splitKey <= kr.Start {
			return "", fmt.Errorf("splitKey %q must be greater than partition start %q", splitKey, kr.Start)
		}
		if kr.End != "" && splitKey >= kr.End {
			return "", fmt.Errorf("splitKey %q must be less than partition end %q", splitKey, kr.End)
		}
	}

	// 5. Use the provided new partition ID, or generate one.
	if newPartitionID == "" {
		newPartitionID = uuid.New().String()
	}

	// 6. Send ExecuteSplit to the source PS. If splitKey=="", the PS decides and returns the actual key.
	psCtrl, err := s.psFactory.GetClient(entry.Node.Address)
	if err != nil {
		return "", fmt.Errorf("connect to source PS %s: %w", entry.Node.Address, err)
	}
	usedKey, err := psCtrl.ExecuteSplit(ctx, entry.Partition.ActorType, partitionID, splitKey, kr.Start, kr.End, newPartitionID)
	if err != nil {
		return "", fmt.Errorf("execute split on PS: %w", err)
	}

	// 7. Update the routing table using the actual splitKey determined by the PS.
	newEntries := buildSplitEntries(rt.Entries(), partitionID, usedKey, newPartitionID, entry)
	newRT, err := domain.NewRoutingTable(rt.Version()+1, newEntries)
	if err != nil {
		return "", fmt.Errorf("build new routing table: %w", err)
	}

	if err := s.routingStore.Save(ctx, newRT); err != nil {
		return "", fmt.Errorf("save routing table: %w", err)
	}

	return newPartitionID, nil
}

// buildSplitEntries removes partitionID from entries and appends two new entries.
func buildSplitEntries(
	entries []domain.RouteEntry,
	partitionID, splitKey, newPartitionID string,
	original domain.RouteEntry,
) []domain.RouteEntry {
	result := make([]domain.RouteEntry, 0, len(entries)+1)
	for _, e := range entries {
		if e.Partition.ID == partitionID {
			continue
		}
		result = append(result, e)
	}

	// Lower half: [original.Start, splitKey)
	result = append(result, domain.RouteEntry{
		Partition: domain.Partition{
			ID:        partitionID,
			ActorType: original.Partition.ActorType,
			KeyRange:  domain.KeyRange{Start: original.Partition.KeyRange.Start, End: splitKey},
		},
		Node:            original.Node,
		PartitionStatus: domain.PartitionStatusActive,
	})

	// Upper half: [splitKey, original.End)
	result = append(result, domain.RouteEntry{
		Partition: domain.Partition{
			ID:        newPartitionID,
			ActorType: original.Partition.ActorType,
			KeyRange:  domain.KeyRange{Start: splitKey, End: original.Partition.KeyRange.End},
		},
		Node:            original.Node,
		PartitionStatus: domain.PartitionStatusActive,
	})

	return result
}
