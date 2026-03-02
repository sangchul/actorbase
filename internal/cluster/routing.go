package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oomymy/actorbase/internal/domain"
)

const routingKey = "/actorbase/routing"

// RoutingTableStore는 라우팅 테이블 저장/조회/감시를 담당하는 인터페이스.
// PM: Save로 라우팅 테이블 갱신.
// PS: Watch로 변경 감지.
type RoutingTableStore interface {
	// Save는 라우팅 테이블을 저장소에 저장한다.
	Save(ctx context.Context, rt *domain.RoutingTable) error

	// Load는 현재 라우팅 테이블을 조회한다.
	// 데이터가 없으면 (nil, nil)을 반환한다.
	Load(ctx context.Context) (*domain.RoutingTable, error)

	// Watch는 라우팅 테이블이 변경될 때마다 새 테이블을 전달하는 채널을 반환한다.
	// Watch 시작 시 현재 테이블을 즉시 한 번 전달한다. (초기 상태 동기화)
	// ctx 취소 시 채널이 닫힌다.
	Watch(ctx context.Context) <-chan *domain.RoutingTable
}

// NewRoutingTableStore는 etcd 기반 RoutingTableStore를 생성한다.
func NewRoutingTableStore(client *clientv3.Client) RoutingTableStore {
	return &etcdRoutingTableStore{client: client}
}

// etcdRoutingTableStore는 RoutingTableStore의 etcd 구현체.
type etcdRoutingTableStore struct {
	client *clientv3.Client
}

func (s *etcdRoutingTableStore) Save(ctx context.Context, rt *domain.RoutingTable) error {
	data, err := marshalRoutingTable(rt)
	if err != nil {
		return fmt.Errorf("marshal routing table: %w", err)
	}
	if _, err := s.client.Put(ctx, routingKey, string(data)); err != nil {
		return fmt.Errorf("save routing table: %w", err)
	}
	return nil
}

func (s *etcdRoutingTableStore) Load(ctx context.Context) (*domain.RoutingTable, error) {
	resp, err := s.client.Get(ctx, routingKey)
	if err != nil {
		return nil, fmt.Errorf("load routing table: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return unmarshalRoutingTable(resp.Kvs[0].Value)
}

func (s *etcdRoutingTableStore) Watch(ctx context.Context) <-chan *domain.RoutingTable {
	ch := make(chan *domain.RoutingTable, 8)
	go s.watchLoop(ctx, ch)
	return ch
}

func (s *etcdRoutingTableStore) watchLoop(ctx context.Context, ch chan *domain.RoutingTable) {
	defer close(ch)

	// 시작 시 현재 테이블을 즉시 전달 (초기 동기화)
	current, err := s.Load(ctx)
	if err != nil {
		slog.Error("routing: initial load failed", "err", err)
		return
	}
	if current != nil {
		select {
		case ch <- current:
		case <-ctx.Done():
			return
		}
	}

	watchCh := s.client.Watch(ctx, routingKey)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				if ctx.Err() != nil {
					return
				}
				// 재연결: 현재 테이블 재전달
				rt, err := s.Load(ctx)
				if err != nil {
					slog.Error("routing: reload after reconnect failed", "err", err)
					return
				}
				if rt != nil {
					select {
					case ch <- rt:
					case <-ctx.Done():
						return
					}
				}
				watchCh = s.client.Watch(ctx, routingKey)
				continue
			}
			if resp.Err() != nil {
				slog.Error("routing: watch error", "err", resp.Err())
				continue
			}
			for _, ev := range resp.Events {
				if ev.Type != clientv3.EventTypePut {
					continue
				}
				rt, err := unmarshalRoutingTable(ev.Kv.Value)
				if err != nil {
					slog.Error("routing: unmarshal failed", "err", err)
					continue
				}
				select {
				case ch <- rt:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// ── 직렬화 ──────────────────────────────────────────────────────────────────

// routingTableDTO는 etcd JSON 직렬화 DTO.
type routingTableDTO struct {
	Version int64           `json:"version"`
	Entries []routeEntryDTO `json:"entries"`
}

type routeEntryDTO struct {
	PartitionID     string `json:"partitionId"`
	KeyRangeStart   string `json:"keyRangeStart"`
	KeyRangeEnd     string `json:"keyRangeEnd"`
	NodeID          string `json:"nodeId"`
	NodeAddress     string `json:"nodeAddress"`
	NodeStatus      int    `json:"nodeStatus"`
	PartitionStatus int    `json:"partitionStatus"`
}

func marshalRoutingTable(rt *domain.RoutingTable) ([]byte, error) {
	entries := rt.Entries()
	dtoEntries := make([]routeEntryDTO, len(entries))
	for i, e := range entries {
		dtoEntries[i] = routeEntryDTO{
			PartitionID:     e.Partition.ID,
			KeyRangeStart:   e.Partition.KeyRange.Start,
			KeyRangeEnd:     e.Partition.KeyRange.End,
			NodeID:          e.Node.ID,
			NodeAddress:     e.Node.Address,
			NodeStatus:      int(e.Node.Status),
			PartitionStatus: int(e.PartitionStatus),
		}
	}
	return json.Marshal(routingTableDTO{Version: rt.Version(), Entries: dtoEntries})
}

func unmarshalRoutingTable(data []byte) (*domain.RoutingTable, error) {
	var dto routingTableDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return nil, fmt.Errorf("unmarshal routing table: %w", err)
	}

	entries := make([]domain.RouteEntry, len(dto.Entries))
	for i, e := range dto.Entries {
		entries[i] = domain.RouteEntry{
			Partition: domain.Partition{
				ID:       e.PartitionID,
				KeyRange: domain.KeyRange{Start: e.KeyRangeStart, End: e.KeyRangeEnd},
			},
			Node: domain.NodeInfo{
				ID:      e.NodeID,
				Address: e.NodeAddress,
				Status:  domain.NodeStatus(e.NodeStatus),
			},
			PartitionStatus: domain.PartitionStatus(e.PartitionStatus),
		}
	}

	return domain.NewRoutingTable(dto.Version, entries)
}
