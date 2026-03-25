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

// NodeRegistry is the interface responsible for registering, deregistering,
// and listing cluster nodes.
// ps/: uses Register and Deregister.
// pm/: uses ListNodes.
type NodeRegistry interface {
	// Register registers node in the cluster and maintains the registration
	// until ctx is cancelled. Returns when ctx is cancelled (the lease expires
	// after its TTL). Call Deregister separately for graceful shutdown.
	Register(ctx context.Context, node domain.NodeInfo) error

	// Deregister immediately removes nodeID from the cluster without waiting
	// for TTL expiry. Used during graceful shutdown.
	Deregister(ctx context.Context, nodeID string) error

	// ListNodes returns all currently registered (alive) nodes.
	ListNodes(ctx context.Context) ([]domain.NodeInfo, error)
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

func (r *etcdNodeRegistry) ListNodes(ctx context.Context) ([]domain.NodeInfo, error) {
	resp, err := r.client.Get(ctx, nodeKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	nodes := make([]domain.NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var dto nodeInfoDTO
		if err := json.Unmarshal(kv.Value, &dto); err != nil {
			return nil, fmt.Errorf("unmarshal node info: %w", err)
		}
		nodes = append(nodes, domain.NodeInfo{
			ID:      dto.ID,
			Address: dto.Address,
			Status:  domain.NodeStatus(dto.Status),
		})
	}
	return nodes, nil
}

// nodeInfoDTO is the serialization DTO for NodeInfo stored as JSON in etcd.
type nodeInfoDTO struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  int    `json:"status"`
}
