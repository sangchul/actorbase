package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newBucket creates an empty bucketActor for testing.
func newBucket() *bucketActor {
	return &bucketActor{buckets: make(map[string]bucketMeta)}
}

func TestBucketActor_Create(t *testing.T) {
	a := newBucket()

	resp, walEntry, err := a.Receive(nil, BucketRequest{Op: "create", Name: "photos", Region: "us-east-1"})

	require.NoError(t, err)
	require.True(t, resp.Found)
	require.Equal(t, "photos", resp.Name)
	require.Equal(t, "us-east-1", resp.Region)
	require.False(t, resp.CreatedAt.IsZero())
	require.NotNil(t, walEntry, "create should produce a WAL entry")
}

func TestBucketActor_Get(t *testing.T) {
	a := newBucket()
	a.Receive(nil, BucketRequest{Op: "create", Name: "photos", Region: "ap-northeast-2"})

	t.Run("existing", func(t *testing.T) {
		resp, walEntry, err := a.Receive(nil, BucketRequest{Op: "get", Name: "photos"})
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, "ap-northeast-2", resp.Region)
		require.Nil(t, walEntry, "get should not produce a WAL entry (read-only)")
	})

	t.Run("not found", func(t *testing.T) {
		resp, _, err := a.Receive(nil, BucketRequest{Op: "get", Name: "videos"})
		require.NoError(t, err)
		require.False(t, resp.Found)
	})
}

func TestBucketActor_Delete(t *testing.T) {
	a := newBucket()
	a.Receive(nil, BucketRequest{Op: "create", Name: "photos", Region: "us-east-1"})

	_, walEntry, err := a.Receive(nil, BucketRequest{Op: "delete", Name: "photos"})
	require.NoError(t, err)
	require.NotNil(t, walEntry, "delete should produce a WAL entry")

	resp, _, _ := a.Receive(nil, BucketRequest{Op: "get", Name: "photos"})
	require.False(t, resp.Found, "bucket should not be found after delete")
}

func TestBucketActor_UnknownOp(t *testing.T) {
	a := newBucket()
	_, _, err := a.Receive(nil, BucketRequest{Op: "invalid"})
	require.Error(t, err)
}

func TestBucketActor_WALReplay(t *testing.T) {
	// WAL replay is the core of crash recovery: applying recorded WAL entries in order reproduces the original state.
	a := newBucket()
	_, walAlpha, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "alpha", Region: "us-east-1"})
	_, walBeta, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "beta", Region: "eu-west-1"})
	_, walGamma, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "gamma", Region: "ap-northeast-2"})
	_, walDeleteBeta, _ := a.Receive(nil, BucketRequest{Op: "delete", Name: "beta"})

	restored := newBucket()
	for _, entry := range [][]byte{walAlpha, walBeta, walGamma, walDeleteBeta} {
		require.NoError(t, restored.Replay(entry))
	}

	// alpha and gamma should exist; beta should not
	for name, shouldExist := range map[string]bool{"alpha": true, "beta": false, "gamma": true} {
		resp, _, _ := restored.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.Equal(t, shouldExist, resp.Found, "bucket %q", name)
	}
}

func TestBucketActor_SnapshotRestore(t *testing.T) {
	// Snapshot/Restore enables fast checkpoint-based recovery.
	// The actor can start from the latest state without replaying the full WAL.
	original := newBucket()
	original.Receive(nil, BucketRequest{Op: "create", Name: "alpha", Region: "us-east-1"})
	original.Receive(nil, BucketRequest{Op: "create", Name: "beta", Region: "eu-west-1"})

	snap, err := original.Export("")
	require.NoError(t, err)

	restored := newBucket()
	require.NoError(t, restored.Import(snap))

	for _, name := range []string{"alpha", "beta"} {
		resp, _, _ := restored.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.True(t, resp.Found, "bucket %q should exist after restore", name)
	}
	require.EqualValues(t, 2, restored.KeyCount())
}

func TestBucketActor_Split(t *testing.T) {
	// Split is the core operation for dividing a partition when traffic or key count exceeds the threshold.
	// Keys >= splitKey are exported to the new upper partition; the rest remain in the current partition.
	a := newBucket()
	a.Receive(nil, BucketRequest{Op: "create", Name: "apple", Region: "us-east-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "banana", Region: "us-east-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "cherry", Region: "eu-west-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "date", Region: "eu-west-1"})

	upperData, err := a.Export("cherry")
	require.NoError(t, err)

	upper := newBucket()
	require.NoError(t, upper.Import(upperData))

	// "cherry", "date" → upper partition
	for _, name := range []string{"cherry", "date"} {
		resp, _, _ := upper.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.True(t, resp.Found, "bucket %q should be in upper partition", name)
	}
	// "apple", "banana" → remain in the lower partition (current actor)
	for _, name := range []string{"apple", "banana"} {
		resp, _, _ := a.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.True(t, resp.Found, "bucket %q should remain in lower partition", name)
	}
	// no duplicates after split
	for _, name := range []string{"cherry", "date"} {
		resp, _, _ := a.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.False(t, resp.Found, "bucket %q should have been moved to upper partition", name)
	}
}

func TestBucketActor_KeyCount(t *testing.T) {
	a := newBucket()
	require.EqualValues(t, 0, a.KeyCount())

	a.Receive(nil, BucketRequest{Op: "create", Name: "alpha", Region: "us-east-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "beta", Region: "eu-west-1"})
	require.EqualValues(t, 2, a.KeyCount())

	a.Receive(nil, BucketRequest{Op: "delete", Name: "alpha"})
	require.EqualValues(t, 1, a.KeyCount())
}

func TestBucketActor_WALEntryFormat(t *testing.T) {
	// Verifies that the WAL entry serialization format is correct.
	// The format must be stable so that Replay produces identical results across instances.
	a := newBucket()
	_, walEntry, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "photos", Region: "us-east-1"})

	var op bucketWALOp
	require.NoError(t, json.Unmarshal(walEntry, &op))
	require.Equal(t, "create", op.Op)
	require.Equal(t, "photos", op.Name)
	require.Equal(t, "us-east-1", op.Meta.Region)
	require.False(t, op.Meta.CreatedAt.IsZero(), "WAL entry should include CreatedAt")
}

func TestBucketActor_CreatedAtPreservedOnReplay(t *testing.T) {
	// On Replay, CreatedAt must be restored to the original timestamp.
	// The WAL-recorded time must be used, not a new time.Now().
	a := newBucket()
	_, walEntry, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "photos", Region: "us-east-1"})
	originalTime := a.buckets["photos"].CreatedAt

	time.Sleep(10 * time.Millisecond)

	restored := newBucket()
	require.NoError(t, restored.Replay(walEntry))
	require.True(t, restored.buckets["photos"].CreatedAt.Equal(originalTime),
		"CreatedAt should be preserved from WAL, not regenerated")
}
