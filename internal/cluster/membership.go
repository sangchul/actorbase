package cluster

import (
	"context"
	"encoding/json"
	"log/slog"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sangchul/actorbase/internal/domain"
)

// NodeEventType은 멤버십 변경 이벤트의 종류.
type NodeEventType int

const (
	NodeJoined NodeEventType = iota // 새 노드가 등록됨
	NodeLeft                        // 노드의 lease가 만료되거나 revoke됨
)

// NodeLeaveReason은 노드 이탈 원인.
// 현재 etcd watch 레벨에서 graceful과 failure를 구분하기 어려우므로
// 항상 NodeLeaveFailure로 전달된다. BalancePolicy 구현체는 라우팅 테이블에
// 파티션이 남아 있는지 확인하여 실제 처리 여부를 결정한다.
type NodeLeaveReason int

const (
	NodeLeaveGraceful NodeLeaveReason = iota
	NodeLeaveFailure
)

// NodeEvent는 클러스터 멤버십 변경 이벤트.
type NodeEvent struct {
	Type   NodeEventType
	Node   domain.NodeInfo
	Reason NodeLeaveReason // NodeLeft일 때만 유효
}

// MembershipWatcher는 노드 join/leave 이벤트를 감지하는 인터페이스.
// PM이 사용한다. 노드 장애 감지 및 파티션 재배치 트리거에 활용한다.
type MembershipWatcher interface {
	// Watch는 노드 join/leave 이벤트 채널을 반환한다.
	// Watch 재연결 시 현재 전체 노드 목록과 비교하여 놓친 이벤트를 보정한다.
	// ctx 취소 시 채널이 닫힌다.
	Watch(ctx context.Context) <-chan NodeEvent
}

// NewMembershipWatcher는 etcd 기반 MembershipWatcher를 생성한다.
func NewMembershipWatcher(client *clientv3.Client) MembershipWatcher {
	return &etcdMembershipWatcher{client: client}
}

// etcdMembershipWatcher는 MembershipWatcher의 etcd 구현체.
type etcdMembershipWatcher struct {
	client *clientv3.Client
}

func (w *etcdMembershipWatcher) Watch(ctx context.Context) <-chan NodeEvent {
	ch := make(chan NodeEvent, 32)
	go w.watchLoop(ctx, ch)
	return ch
}

func (w *etcdMembershipWatcher) watchLoop(ctx context.Context, ch chan NodeEvent) {
	defer close(ch)

	// 현재 알고 있는 노드 목록 (재연결 시 diff 기준)
	known := make(map[string]domain.NodeInfo)

	// 초기 상태 로드 + 첫 이벤트 전달
	current, err := w.listNodes(ctx)
	if err != nil {
		slog.Error("membership: initial list failed", "err", err)
		return
	}
	for _, n := range current {
		known[n.ID] = n
		select {
		case ch <- NodeEvent{Type: NodeJoined, Node: n}:
		case <-ctx.Done():
			return
		}
	}

	watchCh := w.client.Watch(ctx, nodeKeyPrefix, clientv3.WithPrefix())
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				// watch 채널 종료: ctx 취소가 아니면 재연결 시도
				if ctx.Err() != nil {
					return
				}
				// 재연결 후 diff로 놓친 이벤트 보정
				if err := w.reconcile(ctx, known, ch); err != nil {
					return
				}
				watchCh = w.client.Watch(ctx, nodeKeyPrefix, clientv3.WithPrefix())
				continue
			}
			if resp.Err() != nil {
				slog.Error("membership: watch error", "err", resp.Err())
				continue
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					node, err := unmarshalNodeInfo(ev.Kv.Value)
					if err != nil {
						slog.Error("membership: unmarshal node", "err", err)
						continue
					}
					known[node.ID] = node
					select {
					case ch <- NodeEvent{Type: NodeJoined, Node: node}:
					case <-ctx.Done():
						return
					}
				case clientv3.EventTypeDelete:
					nodeID := nodeIDFromKey(string(ev.Kv.Key))
					node, ok := known[nodeID]
					if !ok {
						continue
					}
					delete(known, nodeID)
					select {
					case ch <- NodeEvent{Type: NodeLeft, Node: node, Reason: NodeLeaveFailure}:
					case <-ctx.Done():
						return
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// reconcile은 etcd watch 재연결 후 놓친 이벤트를 보정한다.
// 현재 etcd 상태와 known을 diff하여 join/leave 이벤트를 전달한다.
func (w *etcdMembershipWatcher) reconcile(ctx context.Context, known map[string]domain.NodeInfo, ch chan NodeEvent) error {
	current, err := w.listNodes(ctx)
	if err != nil {
		return err
	}

	currentMap := make(map[string]domain.NodeInfo, len(current))
	for _, n := range current {
		currentMap[n.ID] = n
	}

	// 새로 추가된 노드
	for id, n := range currentMap {
		if _, exists := known[id]; !exists {
			known[id] = n
			select {
			case ch <- NodeEvent{Type: NodeJoined, Node: n}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// 사라진 노드
	for id, n := range known {
		if _, exists := currentMap[id]; !exists {
			delete(known, id)
			select {
			case ch <- NodeEvent{Type: NodeLeft, Node: n}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

func (w *etcdMembershipWatcher) listNodes(ctx context.Context) ([]domain.NodeInfo, error) {
	resp, err := w.client.Get(ctx, nodeKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	nodes := make([]domain.NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		node, err := unmarshalNodeInfo(kv.Value)
		if err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func unmarshalNodeInfo(data []byte) (domain.NodeInfo, error) {
	var dto nodeInfoDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return domain.NodeInfo{}, err
	}
	return domain.NodeInfo{
		ID:      dto.ID,
		Address: dto.Address,
		Status:  domain.NodeStatus(dto.Status),
	}, nil
}

// nodeIDFromKey는 "/actorbase/nodes/{nodeID}" 키에서 nodeID를 추출한다.
func nodeIDFromKey(key string) string {
	if len(key) <= len(nodeKeyPrefix) {
		return ""
	}
	return key[len(nodeKeyPrefix):]
}
