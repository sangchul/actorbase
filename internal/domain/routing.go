package domain

import (
	"fmt"
	"sort"
)

// PartitionStatus is the current operational state of a partition.
type PartitionStatus int

const (
	// PartitionStatusActive: operating normally; accepting requests.
	PartitionStatusActive PartitionStatus = iota

	// PartitionStatusDraining: migration in progress; new requests are rejected (ErrPartitionBusy).
	PartitionStatusDraining
)

// RouteEntry is a pair of a partition and the node that hosts it.
type RouteEntry struct {
	Partition       Partition
	Node            NodeInfo
	PartitionStatus PartitionStatus
}

// RoutingTable is the partition-to-node mapping table.
//
// Internal structure:
//   - byType    : actorType → slice sorted ascending by KeyRange.Start
//                 Key range overlap validation is performed only within the same actorType.
//   - partitions: partitionID → RouteEntry map (O(1) lookup)
//
// Different actorTypes may have overlapping key ranges without conflict.
// (e.g., bucket partitions and object partitions have independent key spaces.)
type RoutingTable struct {
	version    int64
	byType     map[string][]RouteEntry // actorType → sorted []RouteEntry
	partitions map[string]RouteEntry   // partitionID → RouteEntry (O(1) lookup)
}

// NewRoutingTable creates a RoutingTable.
// It groups entries by actorType and sorts each group by KeyRange.Start.
// Returns an error if there are overlapping ranges or duplicate partitionIDs within the same actorType.
func NewRoutingTable(version int64, entries []RouteEntry) (*RoutingTable, error) {
	byType := make(map[string][]RouteEntry)
	partitions := make(map[string]RouteEntry, len(entries))

	// check for duplicate partitionIDs and group by actorType
	for _, e := range entries {
		if _, dup := partitions[e.Partition.ID]; dup {
			return nil, fmt.Errorf("duplicate partitionID: %s", e.Partition.ID)
		}
		partitions[e.Partition.ID] = e
		byType[e.Partition.ActorType] = append(byType[e.Partition.ActorType], e)
	}

	// sort by actorType and check for overlaps
	for actorType, group := range byType {
		sort.Slice(group, func(i, j int) bool {
			return group[i].Partition.KeyRange.Start < group[j].Partition.KeyRange.Start
		})
		for i := 0; i+1 < len(group); i++ {
			if group[i].Partition.KeyRange.Overlaps(group[i+1].Partition.KeyRange) {
				return nil, fmt.Errorf(
					"overlapping key ranges in actor type %q: partition %s [%s, %s) and partition %s [%s, %s)",
					actorType,
					group[i].Partition.ID, group[i].Partition.KeyRange.Start, group[i].Partition.KeyRange.End,
					group[i+1].Partition.ID, group[i+1].Partition.KeyRange.Start, group[i+1].Partition.KeyRange.End,
				)
			}
		}
		byType[actorType] = group
	}

	return &RoutingTable{
		version:    version,
		byType:     byType,
		partitions: partitions,
	}, nil
}

// Version returns the monotonically increasing version number.
func (rt *RoutingTable) Version() int64 {
	return rt.version
}

// Entries returns all RouteEntries sorted by (actorType, KeyRange.Start).
func (rt *RoutingTable) Entries() []RouteEntry {
	actorTypes := rt.ActorTypes()
	var result []RouteEntry
	for _, at := range actorTypes {
		result = append(result, rt.byType[at]...)
	}
	return result
}

// EntriesByType returns the RouteEntries for the given actorType, sorted by KeyRange.Start.
func (rt *RoutingTable) EntriesByType(actorType string) []RouteEntry {
	entries := rt.byType[actorType]
	if len(entries) == 0 {
		return nil
	}
	result := make([]RouteEntry, len(entries))
	copy(result, entries)
	return result
}

// ActorTypes returns all registered actorTypes in sorted order.
func (rt *RoutingTable) ActorTypes() []string {
	types := make([]string, 0, len(rt.byType))
	for at := range rt.byType {
		types = append(types, at)
	}
	sort.Strings(types)
	return types
}

// Lookup returns the RouteEntry of the partition that contains key within the given actorType.
// It binary-searches the sorted entries. O(log n).
// Returns (zero value, false) if no matching partition is found.
func (rt *RoutingTable) Lookup(actorType, key string) (RouteEntry, bool) {
	group := rt.byType[actorType]
	n := len(group)
	if n == 0 {
		return RouteEntry{}, false
	}

	// find the last entry position where Start <= key
	idx := sort.Search(n, func(i int) bool {
		return group[i].Partition.KeyRange.Start > key
	}) - 1

	if idx < 0 {
		return RouteEntry{}, false
	}
	e := group[idx]
	if !e.Partition.KeyRange.Contains(key) {
		return RouteEntry{}, false
	}
	return e, true
}

// LookupByPartition returns the RouteEntry for the given partitionID.
// Performs an O(1) lookup via the internal map.
// Returns (zero value, false) if no matching partition is found.
func (rt *RoutingTable) LookupByPartition(partitionID string) (RouteEntry, bool) {
	e, ok := rt.partitions[partitionID]
	return e, ok
}

// PartitionsInRange returns the partitions within actorType that overlap the [startKey, endKey) range,
// sorted ascending by KeyRange.Start.
// endKey == "" means no upper bound (includes all partitions up to the highest range).
func (rt *RoutingTable) PartitionsInRange(actorType, startKey, endKey string) []RouteEntry {
	group := rt.byType[actorType]
	n := len(group)
	if n == 0 {
		return nil
	}

	// first partition position that could contain startKey: last entry where Start <= startKey
	idx := sort.Search(n, func(i int) bool {
		return group[i].Partition.KeyRange.Start > startKey
	}) - 1
	if idx < 0 {
		idx = 0
	}

	var result []RouteEntry
	for i := idx; i < n; i++ {
		e := group[i]
		kr := e.Partition.KeyRange
		// if the partition's Start is >= the requested endKey, there are no more overlaps
		if endKey != "" && kr.Start >= endKey {
			break
		}
		// if the partition's End is <= startKey, there is no overlap (End=="" means infinity)
		if kr.End != "" && kr.End <= startKey {
			continue
		}
		result = append(result, e)
	}
	return result
}
