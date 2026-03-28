package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const workJournalKeyPrefix = "/actorbase/pm/work/"

// WorkJournal records in-progress PM operations to etcd so that a newly
// elected PM leader can resume incomplete work after a crash.
type WorkJournal interface {
	// Begin records a new pending work item. Returns an error if the work ID
	// already exists (prevents duplicate entries on leader re-election).
	Begin(ctx context.Context, work PendingWork) error

	// Complete removes the work item once it finishes successfully.
	Complete(ctx context.Context, workID string) error

	// ListPending returns all work items that have not yet been completed.
	ListPending(ctx context.Context) ([]PendingWork, error)
}

// PendingWork describes a single in-progress PM operation.
type PendingWork struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`       // "split" | "migrate" | "failover_node" | "merge"
	Params    json.RawMessage `json:"params"`     // operation-specific JSON parameters
	StartedAt time.Time       `json:"started_at"`
}

// Work type constants.
const (
	WorkTypeSplit       = "split"
	WorkTypeMigrate     = "migrate"
	WorkTypeFailover    = "failover_node"
	WorkTypeMerge       = "merge"
)

// SplitParams holds the parameters for a pending split operation.
type SplitParams struct {
	ActorType      string `json:"actor_type"`
	PartitionID    string `json:"partition_id"`
	SplitKey       string `json:"split_key"`
	NewPartitionID string `json:"new_partition_id"` // pre-generated before journal write to enable idempotent resume
}

// MigrateParams holds the parameters for a pending migrate operation.
type MigrateParams struct {
	ActorType    string `json:"actor_type"`
	PartitionID  string `json:"partition_id"`
	TargetNodeID string `json:"target_node_id"`
}

// FailoverNodeParams holds the parameters for a pending node-level failover.
type FailoverNodeParams struct {
	DeadNodeID string `json:"dead_node_id"`
}

// MergeParams holds the parameters for a pending merge operation.
type MergeParams struct {
	ActorType string `json:"actor_type"`
	LowerID   string `json:"lower_id"`
	UpperID   string `json:"upper_id"`
}

// MarshalWorkParams marshals op-specific params into json.RawMessage.
func MarshalWorkParams(v any) (json.RawMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal work params: %w", err)
	}
	return json.RawMessage(data), nil
}

// NewWorkJournal creates an etcd-backed WorkJournal.
func NewWorkJournal(client *clientv3.Client) WorkJournal {
	return &etcdWorkJournal{client: client}
}

type etcdWorkJournal struct {
	client *clientv3.Client
}

func (j *etcdWorkJournal) Begin(ctx context.Context, work PendingWork) error {
	key := workJournalKeyPrefix + work.ID
	data, err := json.Marshal(work)
	if err != nil {
		return fmt.Errorf("marshal pending work: %w", err)
	}
	// Use a transaction to guard against duplicate work IDs.
	txn, err := j.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("begin work journal entry: %w", err)
	}
	if !txn.Succeeded {
		return fmt.Errorf("work journal entry %q already exists", work.ID)
	}
	return nil
}

func (j *etcdWorkJournal) Complete(ctx context.Context, workID string) error {
	key := workJournalKeyPrefix + workID
	if _, err := j.client.Delete(ctx, key); err != nil {
		return fmt.Errorf("complete work journal entry: %w", err)
	}
	return nil
}

func (j *etcdWorkJournal) ListPending(ctx context.Context) ([]PendingWork, error) {
	resp, err := j.client.Get(ctx, workJournalKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list pending work: %w", err)
	}
	works := make([]PendingWork, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var w PendingWork
		if err := json.Unmarshal(kv.Value, &w); err != nil {
			return nil, fmt.Errorf("unmarshal pending work: %w", err)
		}
		works = append(works, w)
	}
	return works, nil
}
