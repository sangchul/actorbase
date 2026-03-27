package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sangchul/actorbase/internal/domain"
)

const nodeKeyPrefix = "/actorbase/nodes/"

// NodeRegistry manages the PS heartbeat keys (TTL-based liveness).
// PS uses Register/Deregister; PM uses ListLiveNodeIDs for reconciliation.
// For the authoritative node list and state management, see NodeCatalog.
type NodeRegistry interface {
	// Register writes a TTL-leased heartbeat key for node and blocks until ctx is cancelled.
	// Call Deregister separately for graceful shutdown to revoke the lease immediately.
	Register(ctx context.Context, node domain.NodeInfo) error

	// Deregister immediately revokes the heartbeat lease for nodeID.
	Deregister(ctx context.Context, nodeID string) error

	// ListLiveNodeIDs returns the IDs of nodes that currently have a live heartbeat key.
	// Used by PM on startup to reconcile catalog state against live heartbeats.
	ListLiveNodeIDs(ctx context.Context) ([]string, error)
}

// NewNodeRegistry creates an etcd-based NodeRegistry.
func NewNodeRegistry(client *clientv3.Client, ttl time.Duration) NodeRegistry {
	return &etcdNodeRegistry{client: client, ttl: ttl}
}

// etcdNodeRegistry is the etcd implementation of NodeRegistry.
type etcdNodeRegistry struct {
	client *clientv3.Client
	ttl    time.Duration
}

func (r *etcdNodeRegistry) Register(ctx context.Context, node domain.NodeInfo) error {
	data, err := json.Marshal(nodeInfoDTO{
		ID:      node.ID,
		Address: node.Address,
		Status:  int(node.Status),
	})
	if err != nil {
		return fmt.Errorf("marshal node info: %w", err)
	}

	ttlSec := int64(r.ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	leaseResp, err := r.client.Grant(ctx, ttlSec)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}

	key := nodeKeyPrefix + node.ID
	if _, err := r.client.Put(ctx, key, string(data), clientv3.WithLease(leaseResp.ID)); err != nil {
		return fmt.Errorf("register node: %w", err)
	}

	keepAliveCh, err := r.client.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return fmt.Errorf("keepalive: %w", err)
	}

	// Drain keepalive responses (the channel blocks if not consumed).
	go func() {
		for range keepAliveCh {
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *etcdNodeRegistry) ListLiveNodeIDs(ctx context.Context) ([]string, error) {
	resp, err := r.client.Get(ctx, nodeKeyPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("list live nodes: %w", err)
	}
	ids := make([]string, 0, len(resp.Kvs))
	prefix := nodeKeyPrefix
	for _, kv := range resp.Kvs {
		id := string(kv.Key)[len(prefix):]
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *etcdNodeRegistry) Deregister(ctx context.Context, nodeID string) error {
	key := nodeKeyPrefix + nodeID
	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get node key: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)
	if leaseID == 0 {
		_, err = r.client.Delete(ctx, key)
		return err
	}
	_, err = r.client.Revoke(ctx, leaseID)
	return err
}

// nodeInfoDTO is the serialization DTO for NodeInfo stored as JSON in etcd.
type nodeInfoDTO struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  int    `json:"status"`
}

// ── NodeCatalog ───────────────────────────────────────────────────────────────

const nodeCatalogKeyPrefix = "/actorbase/node_catalog/"

// NodeCatalog is the PM-managed persistent registry of all cluster nodes.
// Unlike NodeRegistry (which only tracks live heartbeats), NodeCatalog holds
// all nodes in all four states: Waiting, Active, Draining, Failed.
// Entries persist across PS restarts and PM failovers.
type NodeCatalog interface {
	// AddNode registers a new node with Waiting status.
	// Returns an error if the node already exists.
	AddNode(ctx context.Context, node domain.NodeInfo) error

	// UpdateStatus changes the status of an existing node.
	UpdateStatus(ctx context.Context, nodeID string, status domain.NodeStatus) error

	// RemoveNode permanently deletes the node from the catalog.
	// Only nodes in Waiting or Failed state may be removed.
	RemoveNode(ctx context.Context, nodeID string) error

	// GetNode returns the node info for nodeID.
	// found is false if the node does not exist in the catalog.
	GetNode(ctx context.Context, nodeID string) (node domain.NodeInfo, found bool, err error)

	// ListNodes returns all nodes regardless of state.
	ListNodes(ctx context.Context) ([]domain.NodeInfo, error)
}

// NewNodeCatalog creates an etcd-based NodeCatalog.
func NewNodeCatalog(client *clientv3.Client) NodeCatalog {
	return &etcdNodeCatalog{client: client}
}

type etcdNodeCatalog struct {
	client *clientv3.Client
}

func (c *etcdNodeCatalog) AddNode(ctx context.Context, node domain.NodeInfo) error {
	key := nodeCatalogKeyPrefix + node.ID
	// Use a transaction to ensure the key does not already exist.
	data, err := json.Marshal(nodeInfoDTO{ID: node.ID, Address: node.Address, Status: int(node.Status)})
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	txn, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("add node: %w", err)
	}
	if !txn.Succeeded {
		return fmt.Errorf("node %q already exists in catalog", node.ID)
	}
	return nil
}

func (c *etcdNodeCatalog) UpdateStatus(ctx context.Context, nodeID string, status domain.NodeStatus) error {
	key := nodeCatalogKeyPrefix + nodeID
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get node: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("node %q not found in catalog", nodeID)
	}
	var dto nodeInfoDTO
	if err := json.Unmarshal(resp.Kvs[0].Value, &dto); err != nil {
		return fmt.Errorf("unmarshal node: %w", err)
	}
	dto.Status = int(status)
	data, err := json.Marshal(dto)
	if err != nil {
		return fmt.Errorf("marshal node: %w", err)
	}
	if _, err := c.client.Put(ctx, key, string(data)); err != nil {
		return fmt.Errorf("update node status: %w", err)
	}
	return nil
}

func (c *etcdNodeCatalog) RemoveNode(ctx context.Context, nodeID string) error {
	key := nodeCatalogKeyPrefix + nodeID
	if _, err := c.client.Delete(ctx, key); err != nil {
		return fmt.Errorf("remove node: %w", err)
	}
	return nil
}

func (c *etcdNodeCatalog) GetNode(ctx context.Context, nodeID string) (domain.NodeInfo, bool, error) {
	key := nodeCatalogKeyPrefix + nodeID
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return domain.NodeInfo{}, false, fmt.Errorf("get node: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return domain.NodeInfo{}, false, nil
	}
	var dto nodeInfoDTO
	if err := json.Unmarshal(resp.Kvs[0].Value, &dto); err != nil {
		return domain.NodeInfo{}, false, fmt.Errorf("unmarshal node: %w", err)
	}
	return domain.NodeInfo{ID: dto.ID, Address: dto.Address, Status: domain.NodeStatus(dto.Status)}, true, nil
}

func (c *etcdNodeCatalog) ListNodes(ctx context.Context) ([]domain.NodeInfo, error) {
	resp, err := c.client.Get(ctx, nodeCatalogKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	nodes := make([]domain.NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var dto nodeInfoDTO
		if err := json.Unmarshal(kv.Value, &dto); err != nil {
			return nil, fmt.Errorf("unmarshal node: %w", err)
		}
		nodes = append(nodes, domain.NodeInfo{ID: dto.ID, Address: dto.Address, Status: domain.NodeStatus(dto.Status)})
	}
	return nodes, nil
}
