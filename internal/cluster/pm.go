package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	pmKeyPrefix   = "/actorbase/pm/"
	pmLeaseTTL    = 10 // seconds
	pmWaitTimeout = 10 * time.Second
)

// RegisterPM은 PM을 etcd에 lease로 등록한다.
// 등록 성공 후 즉시 반환하며, keepalive와 ctx 취소 시 revoke는 내부 goroutine에서 처리한다.
// PM Start() 초반에 호출한다.
func RegisterPM(ctx context.Context, client *clientv3.Client, pmAddr string) error {
	leaseResp, err := client.Grant(ctx, pmLeaseTTL)
	if err != nil {
		return fmt.Errorf("pm presence: grant lease: %w", err)
	}

	key := pmKeyPrefix + pmAddr
	if _, err := client.Put(ctx, key, pmAddr, clientv3.WithLease(leaseResp.ID)); err != nil {
		return fmt.Errorf("pm presence: register: %w", err)
	}

	keepAliveCh, err := client.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return fmt.Errorf("pm presence: keepalive: %w", err)
	}

	// keepalive 응답 소비 + ctx 취소 시 revoke
	go func() {
		for range keepAliveCh {
		}
		revokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.Revoke(revokeCtx, leaseResp.ID)
	}()

	return nil
}

// GetPMAddr는 etcd에서 PM 주소를 조회한다. PS의 drainPartitions에서 사용한다.
func GetPMAddr(ctx context.Context, client *clientv3.Client) (string, error) {
	resp, err := client.Get(ctx, pmKeyPrefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return "", fmt.Errorf("get pm addr: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no PM registered in etcd")
	}
	return string(resp.Kvs[0].Value), nil
}

// WaitForPM은 etcd에 PM이 등록될 때까지 최대 pmWaitTimeout 동안 기다린다.
// PM이 없으면 에러를 반환한다. PS Start() 초반에 호출한다.
func WaitForPM(ctx context.Context, client *clientv3.Client) error {
	waitCtx, cancel := context.WithTimeout(ctx, pmWaitTimeout)
	defer cancel()

	// 이미 PM이 있으면 즉시 반환
	resp, err := client.Get(waitCtx, pmKeyPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return fmt.Errorf("ps: check pm presence: %w", err)
	}
	if resp.Count > 0 {
		return nil
	}

	// PM이 없으면 watch로 대기
	slog.Warn("ps: PM not found in etcd, waiting...", "timeout", pmWaitTimeout)
	watchCh := client.Watch(waitCtx, pmKeyPrefix, clientv3.WithPrefix())
	for {
		select {
		case wresp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("ps: PM watch closed before PM appeared")
			}
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypePut {
					slog.Info("ps: PM detected, continuing startup")
					return nil
				}
			}
		case <-waitCtx.Done():
			return fmt.Errorf("ps: PM did not appear within %s; start PM first", pmWaitTimeout)
		}
	}
}
