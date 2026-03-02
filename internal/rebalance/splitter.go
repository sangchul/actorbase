package rebalance

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/oomymy/actorbase/internal/cluster"
	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/transport"
)

// Splitter는 파티션 split을 조율한다.
type Splitter struct {
	routingStore cluster.RoutingTableStore
	connPool     *transport.ConnPool
}

// NewSplitter는 Splitter를 생성한다.
func NewSplitter(routingStore cluster.RoutingTableStore, connPool *transport.ConnPool) *Splitter {
	return &Splitter{
		routingStore: routingStore,
		connPool:     connPool,
	}
}

// Split은 partitionID를 splitKey 기준으로 둘로 나눈다.
// 새로 생성된 파티션(상위 절반)의 ID를 반환한다.
// split 후 두 파티션은 기존과 동일한 노드에 위치한다.
func (s *Splitter) Split(ctx context.Context, partitionID, splitKey string) (string, error) {
	// 1. 현재 라우팅 테이블 조회
	rt, err := s.routingStore.Load(ctx)
	if err != nil {
		return "", fmt.Errorf("load routing table: %w", err)
	}
	if rt == nil {
		return "", fmt.Errorf("routing table is empty")
	}

	// 2. 파티션 존재 검증
	entry, ok := rt.LookupByPartition(partitionID)
	if !ok {
		return "", fmt.Errorf("partition %s not found in routing table", partitionID)
	}

	// 3. splitKey 유효성 검증
	kr := entry.Partition.KeyRange
	if splitKey <= kr.Start {
		return "", fmt.Errorf("splitKey %q must be greater than partition start %q", splitKey, kr.Start)
	}
	if kr.End != "" && splitKey >= kr.End {
		return "", fmt.Errorf("splitKey %q must be less than partition end %q", splitKey, kr.End)
	}

	// 4. newPartitionID 생성
	newPartitionID := uuid.New().String()

	// 5. Source PS에 ExecuteSplit 명령
	conn, err := s.connPool.Get(entry.Node.Address)
	if err != nil {
		return "", fmt.Errorf("connect to source PS %s: %w", entry.Node.Address, err)
	}
	psCtrl := transport.NewPSControlClient(conn)
	if err := psCtrl.ExecuteSplit(ctx, partitionID, splitKey, newPartitionID); err != nil {
		return "", fmt.Errorf("execute split on PS: %w", err)
	}

	// 6. 라우팅 테이블 갱신
	// 기존 [start, end) → 두 파티션: [start, splitKey) + [splitKey, end)
	newEntries := buildSplitEntries(rt.Entries(), partitionID, splitKey, newPartitionID, entry)
	newRT, err := domain.NewRoutingTable(rt.Version()+1, newEntries)
	if err != nil {
		return "", fmt.Errorf("build new routing table: %w", err)
	}

	if err := s.routingStore.Save(ctx, newRT); err != nil {
		return "", fmt.Errorf("save routing table: %w", err)
	}

	return newPartitionID, nil
}

// buildSplitEntries는 기존 entries에서 partitionID를 제거하고 두 개의 새 엔트리를 추가한다.
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

	// 하위 절반: [original.Start, splitKey)
	result = append(result, domain.RouteEntry{
		Partition: domain.Partition{
			ID:       partitionID,
			KeyRange: domain.KeyRange{Start: original.Partition.KeyRange.Start, End: splitKey},
		},
		Node:            original.Node,
		PartitionStatus: domain.PartitionStatusActive,
	})

	// 상위 절반: [splitKey, original.End)
	result = append(result, domain.RouteEntry{
		Partition: domain.Partition{
			ID:       newPartitionID,
			KeyRange: domain.KeyRange{Start: splitKey, End: original.Partition.KeyRange.End},
		},
		Node:            original.Node,
		PartitionStatus: domain.PartitionStatusActive,
	})

	return result
}
