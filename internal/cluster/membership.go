package cluster

import (
	"context"
	"encoding/json"
	"log/slog"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sangchul/actorbase/internal/domain"
)

// NodeEventType is the type of a membership change event.
type NodeEventType int

const (
	NodeJoined NodeEventType = iota // A new node has been registered.
	NodeLeft                        // A node's lease has expired or been revoked.
)

// NodeLeaveReason is the reason a node left the cluster.
// Because it is difficult to distinguish between graceful and failure at the
// etcd watch level, NodeLeaveFailure is always delivered. BalancePolicy
// implementations decide whether to act by checking whether partitions remain
// in the routing table.
type NodeLeaveReason int

const (
	NodeLeaveGraceful NodeLeaveReason = iota
	NodeLeaveFailure
)

// NodeEvent is a cluster membership change event.
type NodeEvent struct {
	Type   NodeEventType
	Node   domain.NodeInfo
	Reason NodeLeaveReason // Only valid when Type == NodeLeft.
}

// MembershipWatcher is the interface for detecting node join/leave events.
// Used by the PM to detect node failures and trigger partition rebalancing.
type MembershipWatcher interface {
	// Watch returns a channel of node join/leave events.
	// On reconnect it reconciles against the current node list to recover
	// any missed events. The channel is closed when ctx is cancelled.
	Watch(ctx context.Context) <-chan NodeEvent
}

// NewMembershipWatcher creates an etcd-based MembershipWatcher.
func NewMembershipWatcher(client *clientv3.Client) MembershipWatcher {
	return &etcdMembershipWatcher{client: client}
}

// etcdMembershipWatcher is the etcd implementation of MembershipWatcher.
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

	// Known node list used as the baseline for diffing on reconnect.
	known := make(map[string]domain.NodeInfo)

	// Load the initial state and deliver the first set of events.
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
				// Watch channel closed: reconnect unless ctx was cancelled.
				if ctx.Err() != nil {
					return
				}
				// Reconcile missed events via diff after reconnect.
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

// reconcile recovers missed events after an etcd watch reconnect.
// It diffs the current etcd state against known and delivers join/leave events.
func (w *etcdMembershipWatcher) reconcile(ctx context.Context, known map[string]domain.NodeInfo, ch chan NodeEvent) error {
	current, err := w.listNodes(ctx)
	if err != nil {
		return err
	}

	currentMap := make(map[string]domain.NodeInfo, len(current))
	for _, n := range current {
		currentMap[n.ID] = n
	}

	// Nodes that were newly added.
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

	// Nodes that have disappeared.
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

// nodeIDFromKey extracts the nodeID from a key of the form "/actorbase/nodes/{nodeID}".
func nodeIDFromKey(key string) string {
	if len(key) <= len(nodeKeyPrefix) {
		return ""
	}
	return key[len(nodeKeyPrefix):]
}
