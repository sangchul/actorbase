package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	pmElectionKey = "/actorbase/pm/election"
	pmLeaseTTL    = 10 // seconds
	pmWaitTimeout = 30 * time.Second
)

// CampaignLeader는 etcd election을 통해 PM 리더 자리를 획득한다.
// 리더가 될 때까지 블로킹한다. ctx가 취소되면 에러를 반환한다.
//
// 반환된 Session은 PM 정상 종료 시 Close를 호출해 리더십을 반납해야 한다.
// Session이 닫히면 standby PM 중 하나가 자동으로 새 리더로 선출된다.
func CampaignLeader(ctx context.Context, client *clientv3.Client, pmAddr string) (*concurrency.Session, error) {
	sess, err := concurrency.NewSession(client, concurrency.WithTTL(pmLeaseTTL))
	if err != nil {
		return nil, fmt.Errorf("pm election: create session: %w", err)
	}

	election := concurrency.NewElection(sess, pmElectionKey)

	slog.Info("pm: campaigning for leadership...", "addr", pmAddr)
	if err := election.Campaign(ctx, pmAddr); err != nil {
		sess.Close() //nolint:errcheck
		return nil, fmt.Errorf("pm election: campaign: %w", err)
	}

	slog.Info("pm: elected as leader", "addr", pmAddr)
	return sess, nil
}

// GetLeaderAddr는 etcd에서 현재 PM 리더 주소를 조회한다.
// 리더가 없으면 에러를 반환한다.
func GetLeaderAddr(ctx context.Context, client *clientv3.Client) (string, error) {
	// etcd election은 {electionKey}/{leaseID} 형태의 키를 생성한다.
	// 리더는 CreateRevision이 가장 작은 키다.
	resp, err := client.Get(ctx, pmElectionKey+"/",
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
		clientv3.WithLimit(1))
	if err != nil {
		return "", fmt.Errorf("get leader addr: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no PM leader elected")
	}
	return string(resp.Kvs[0].Value), nil
}

// WaitForLeader는 PM 리더가 선출될 때까지 최대 pmWaitTimeout 동안 대기한다.
// 리더 주소를 반환한다. PS Start() 초반에 호출한다.
func WaitForLeader(ctx context.Context, client *clientv3.Client) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, pmWaitTimeout)
	defer cancel()

	// 이미 리더가 있으면 즉시 반환
	if addr, err := GetLeaderAddr(waitCtx, client); err == nil {
		return addr, nil
	}

	// 없으면 watch로 대기
	slog.Warn("ps: PM leader not found, waiting...", "timeout", pmWaitTimeout)
	watchCh := client.Watch(waitCtx, pmElectionKey+"/", clientv3.WithPrefix())
	for {
		select {
		case wresp, ok := <-watchCh:
			if !ok {
				return "", fmt.Errorf("ps: PM leader watch closed before leader appeared")
			}
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypePut {
					addr := string(ev.Kv.Value)
					slog.Info("ps: PM leader detected", "addr", addr)
					return addr, nil
				}
			}
		case <-waitCtx.Done():
			return "", fmt.Errorf("ps: PM leader did not appear within %s; start PM first", pmWaitTimeout)
		}
	}
}
