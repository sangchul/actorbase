package rebalance

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
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
// actorType은 partitionID의 실제 actor type과 일치해야 한다. 불일치 시 에러를 반환한다.
// 새로 생성된 파티션(상위 절반)의 ID를 반환한다.
// split 후 두 파티션은 기존과 동일한 노드에 위치한다.
func (s *Splitter) Split(ctx context.Context, actorType, partitionID, splitKey string) (string, error) {
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

	// 3. actor type 검증
	if entry.Partition.ActorType != actorType {
		return "", fmt.Errorf("actor type mismatch: partition %s has actor type %q, got %q",
			partitionID, entry.Partition.ActorType, actorType)
	}

	// 4. splitKey 유효성 검증 (명시적으로 제공된 경우에만)
	kr := entry.Partition.KeyRange
	if splitKey != "" {
		if splitKey <= kr.Start {
			return "", fmt.Errorf("splitKey %q must be greater than partition start %q", splitKey, kr.Start)
		}
		if kr.End != "" && splitKey >= kr.End {
			return "", fmt.Errorf("splitKey %q must be less than partition end %q", splitKey, kr.End)
		}
	}

	// 5. newPartitionID 생성
	newPartitionID := uuid.New().String()

	// 6. Source PS에 ExecuteSplit 명령. splitKey=""이면 PS가 결정하고 실제 key를 반환한다.
	conn, err := s.connPool.Get(entry.Node.Address)
	if err != nil {
		return "", fmt.Errorf("connect to source PS %s: %w", entry.Node.Address, err)
	}
	psCtrl := transport.NewPSControlClient(conn)
	usedKey, err := psCtrl.ExecuteSplit(ctx, entry.Partition.ActorType, partitionID, splitKey, kr.Start, kr.End, newPartitionID)
	if err != nil {
		return "", fmt.Errorf("execute split on PS: %w", err)
	}

	// 7. 라우팅 테이블 갱신: PS가 결정한 실제 splitKey 사용
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
			ID:        partitionID,
			ActorType: original.Partition.ActorType,
			KeyRange:  domain.KeyRange{Start: original.Partition.KeyRange.Start, End: splitKey},
		},
		Node:            original.Node,
		PartitionStatus: domain.PartitionStatusActive,
	})

	// 상위 절반: [splitKey, original.End)
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
