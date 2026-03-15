package domain

import (
	"fmt"
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
//   - byType    : actorType → KeyRange.Start 기준 오름차순 정렬 슬라이스
//                 동일 actorType 내에서만 키 범위 중복 검증을 수행한다.
//   - partitions: partitionID → RouteEntry map (O(1) 조회)
//
// actorType이 다르면 키 범위가 겹쳐도 무방하다.
// (bucket 파티션과 object 파티션은 독립적인 키 공간을 가진다.)
type RoutingTable struct {
	version    int64
	byType     map[string][]RouteEntry // actorType → sorted []RouteEntry
	partitions map[string]RouteEntry   // partitionID → RouteEntry (O(1) 조회)
}

// NewRoutingTable은 RoutingTable을 생성한다.
// entries를 actorType별로 그룹화하고, 각 그룹 내에서 KeyRange.Start 기준으로 정렬한다.
// 동일 actorType 내에 겹치는 범위나 중복 partitionID가 있으면 에러를 반환한다.
func NewRoutingTable(version int64, entries []RouteEntry) (*RoutingTable, error) {
	byType := make(map[string][]RouteEntry)
	partitions := make(map[string]RouteEntry, len(entries))

	// partitionID 중복 검사 및 actorType별 그룹화
	for _, e := range entries {
		if _, dup := partitions[e.Partition.ID]; dup {
			return nil, fmt.Errorf("duplicate partitionID: %s", e.Partition.ID)
		}
		partitions[e.Partition.ID] = e
		byType[e.Partition.ActorType] = append(byType[e.Partition.ActorType], e)
	}

	// actorType별 정렬 및 겹침 검사
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

// Version은 단조 증가하는 버전 번호를 반환한다.
func (rt *RoutingTable) Version() int64 {
	return rt.version
}

// Entries는 모든 RouteEntry를 (actorType, KeyRange.Start) 기준으로 정렬하여 반환한다.
func (rt *RoutingTable) Entries() []RouteEntry {
	actorTypes := rt.ActorTypes()
	var result []RouteEntry
	for _, at := range actorTypes {
		result = append(result, rt.byType[at]...)
	}
	return result
}

// EntriesByType은 주어진 actorType의 RouteEntry를 KeyRange.Start 기준으로 정렬하여 반환한다.
func (rt *RoutingTable) EntriesByType(actorType string) []RouteEntry {
	entries := rt.byType[actorType]
	if len(entries) == 0 {
		return nil
	}
	result := make([]RouteEntry, len(entries))
	copy(result, entries)
	return result
}

// ActorTypes는 등록된 모든 actorType을 정렬하여 반환한다.
func (rt *RoutingTable) ActorTypes() []string {
	types := make([]string, 0, len(rt.byType))
	for at := range rt.byType {
		types = append(types, at)
	}
	sort.Strings(types)
	return types
}

// Lookup은 actorType 내에서 key를 포함하는 파티션의 RouteEntry를 반환한다.
// 정렬된 entries를 이진 탐색한다. O(log n).
// 해당하는 파티션이 없으면 (zero value, false)를 반환한다.
func (rt *RoutingTable) Lookup(actorType, key string) (RouteEntry, bool) {
	group := rt.byType[actorType]
	n := len(group)
	if n == 0 {
		return RouteEntry{}, false
	}

	// Start <= key 를 만족하는 마지막 entry 위치를 찾는다.
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

// LookupByPartition은 partitionID에 해당하는 RouteEntry를 반환한다.
// 내부 map을 통해 O(1)로 조회한다.
// 해당하는 파티션이 없으면 (zero value, false)를 반환한다.
func (rt *RoutingTable) LookupByPartition(partitionID string) (RouteEntry, bool) {
	e, ok := rt.partitions[partitionID]
	return e, ok
}

// PartitionsInRange는 actorType 내에서 [startKey, endKey) 범위와 겹치는 파티션을
// KeyRange.Start 기준 오름차순으로 반환한다.
// endKey == ""이면 상한 없음 (모든 파티션의 상위 범위까지 포함).
func (rt *RoutingTable) PartitionsInRange(actorType, startKey, endKey string) []RouteEntry {
	group := rt.byType[actorType]
	n := len(group)
	if n == 0 {
		return nil
	}

	// startKey를 포함할 수 있는 첫 파티션 위치: Start <= startKey인 마지막 항목
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
		// 파티션 Start가 요청 endKey 이상이면 더 이상 겹치지 않음
		if endKey != "" && kr.Start >= endKey {
			break
		}
		// 파티션 End가 startKey 이하이면 겹치지 않음 (End==""은 무한대)
		if kr.End != "" && kr.End <= startKey {
			continue
		}
		result = append(result, e)
	}
	return result
}
