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

// CampaignLeader acquires the PM leader role via an etcd election.
// It blocks until this instance becomes the leader. Returns an error if ctx
// is cancelled before leadership is acquired.
//
// The caller must call Close on the returned Session during a graceful PM
// shutdown to relinquish leadership. Closing the session causes one of the
// standby PMs to be automatically elected as the new leader.
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

// GetLeaderAddr looks up the current PM leader address from etcd.
// Returns an error if no leader has been elected.
func GetLeaderAddr(ctx context.Context, client *clientv3.Client) (string, error) {
	// The etcd election creates keys of the form {electionKey}/{leaseID}.
	// The leader is the key with the smallest CreateRevision.
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

// WaitForLeader waits up to pmWaitTimeout for a PM leader to be elected.
// Returns the leader address. Called early in PS Start().
func WaitForLeader(ctx context.Context, client *clientv3.Client) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, pmWaitTimeout)
	defer cancel()

	// Return immediately if a leader already exists.
	if addr, err := GetLeaderAddr(waitCtx, client); err == nil {
		return addr, nil
	}

	// Otherwise wait via watch.
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
