package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oomymy/actorbase/internal/domain"
)

const nodeKeyPrefix = "/actorbase/nodes/"

// NodeRegistry는 클러스터 노드 등록/해제/조회를 담당하는 인터페이스.
// ps/: Register, Deregister 사용.
// pm/: ListNodes 사용.
type NodeRegistry interface {
	// Register는 node를 클러스터에 등록하고, ctx가 취소될 때까지 등록 상태를 유지한다.
	// ctx 취소 시 반환한다. (lease는 TTL 후 자동 만료)
	// graceful shutdown 시에는 Deregister를 별도로 호출한다.
	Register(ctx context.Context, node domain.NodeInfo) error

	// Deregister는 nodeID를 즉시 클러스터에서 제거한다.
	// graceful shutdown 시 TTL 만료를 기다리지 않고 즉시 제거한다.
	Deregister(ctx context.Context, nodeID string) error

	// ListNodes는 현재 등록된 (살아있는) 모든 노드를 반환한다.
	ListNodes(ctx context.Context) ([]domain.NodeInfo, error)
}

// NewNodeRegistry는 etcd 기반 NodeRegistry를 생성한다.
func NewNodeRegistry(client *clientv3.Client, ttl time.Duration) NodeRegistry {
	return &etcdNodeRegistry{client: client, ttl: ttl}
}

// etcdNodeRegistry는 NodeRegistry의 etcd 구현체.
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

	// keepalive 응답 소비 (소비하지 않으면 채널이 블로킹됨)
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

// nodeInfoDTO는 etcd에 JSON으로 저장되는 NodeInfo 직렬화 DTO.
type nodeInfoDTO struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  int    `json:"status"`
}
