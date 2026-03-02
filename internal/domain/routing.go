package domain

import (
	"fmt"
	"slices"
	"sort"
)

// PartitionStatus는 파티션의 현재 운영 상태.
type PartitionStatus int

const (
	// PartitionStatusActive: 정상 운영 중. 요청 수락.
	PartitionStatusActive PartitionStatus = iota

	// PartitionStatusDraining: migration 진행 중. 새 요청 거부(ErrPartitionBusy).
	PartitionStatusDraining
)

// RouteEntry는 파티션과 그 파티션을 호스팅하는 노드의 쌍.
type RouteEntry struct {
	Partition       Partition
	Node            NodeInfo
	PartitionStatus PartitionStatus
}

// RoutingTable은 파티션→노드 매핑 테이블.
//
// 내부 구조:
//   - entries   : KeyRange.Start 기준 오름차순 정렬 슬라이스 → 키 기반 Lookup O(log n)
//   - partitions: partitionID → entries 인덱스 map          → 파티션 기반 Lookup O(1)
//
// 두 자료구조의 동기화를 보장하기 위해 직접 생성을 금지하고
// NewRoutingTable 생성자를 통해서만 만든다.
type RoutingTable struct {
	version    int64
	entries    []RouteEntry
	partitions map[string]int // partitionID → entries 슬라이스 인덱스
}

// NewRoutingTable은 RoutingTable을 생성한다.
// entries를 KeyRange.Start 기준으로 정렬하고,
// 겹치는 범위가 없는지 검증한 뒤 partition 인덱스 맵을 구축한다.
// 겹치는 범위가 있거나 중복 partitionID가 있으면 에러를 반환한다.
func NewRoutingTable(version int64, entries []RouteEntry) (*RoutingTable, error) {
	sorted := make([]RouteEntry, len(entries))
	copy(sorted, entries)

	slices.SortFunc(sorted, func(a, b RouteEntry) int {
		if a.Partition.KeyRange.Start < b.Partition.KeyRange.Start {
			return -1
		}
		if a.Partition.KeyRange.Start > b.Partition.KeyRange.Start {
			return 1
		}
		return 0
	})

	partitions := make(map[string]int, len(sorted))
	for i, e := range sorted {
		if _, dup := partitions[e.Partition.ID]; dup {
			return nil, fmt.Errorf("duplicate partitionID: %s", e.Partition.ID)
		}
		partitions[e.Partition.ID] = i

		if i+1 < len(sorted) && e.Partition.KeyRange.Overlaps(sorted[i+1].Partition.KeyRange) {
			return nil, fmt.Errorf(
				"overlapping key ranges: partition %s [%s, %s) and partition %s [%s, %s)",
				e.Partition.ID, e.Partition.KeyRange.Start, e.Partition.KeyRange.End,
				sorted[i+1].Partition.ID, sorted[i+1].Partition.KeyRange.Start, sorted[i+1].Partition.KeyRange.End,
			)
		}
	}

	return &RoutingTable{
		version:    version,
		entries:    sorted,
		partitions: partitions,
	}, nil
}

// Version은 단조 증가하는 버전 번호를 반환한다.
func (rt *RoutingTable) Version() int64 {
	return rt.version
}

// Entries는 정렬된 RouteEntry 슬라이스의 복사본을 반환한다.
func (rt *RoutingTable) Entries() []RouteEntry {
	result := make([]RouteEntry, len(rt.entries))
	copy(result, rt.entries)
	return result
}

// Lookup은 key를 포함하는 파티션의 RouteEntry를 반환한다.
// 정렬된 entries를 이진 탐색한다. O(log n).
// 해당하는 파티션이 없으면 (zero value, false)를 반환한다.
func (rt *RoutingTable) Lookup(key string) (RouteEntry, bool) {
	// Start <= key 를 만족하는 마지막 entry 위치를 찾는다.
	n := len(rt.entries)
	idx := sort.Search(n, func(i int) bool {
		return rt.entries[i].Partition.KeyRange.Start > key
	}) - 1

	if idx < 0 {
		return RouteEntry{}, false
	}
	e := rt.entries[idx]
	if !e.Partition.KeyRange.Contains(key) {
		return RouteEntry{}, false
	}
	return e, true
}

// LookupByPartition은 partitionID에 해당하는 RouteEntry를 반환한다.
// 내부 map을 통해 O(1)로 조회한다.
// 해당하는 파티션이 없으면 (zero value, false)를 반환한다.
func (rt *RoutingTable) LookupByPartition(partitionID string) (RouteEntry, bool) {
	idx, ok := rt.partitions[partitionID]
	if !ok {
		return RouteEntry{}, false
	}
	return rt.entries[idx], true
}
