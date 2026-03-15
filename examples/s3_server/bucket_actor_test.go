package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newBucket은 테스트용 빈 bucketActor를 생성한다.
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
	// WAL replay는 장애 복구의 핵심: 기록된 WAL을 순서대로 적용하면 원본 상태가 재현된다.
	a := newBucket()
	_, walAlpha, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "alpha", Region: "us-east-1"})
	_, walBeta, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "beta", Region: "eu-west-1"})
	_, walGamma, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "gamma", Region: "ap-northeast-2"})
	_, walDeleteBeta, _ := a.Receive(nil, BucketRequest{Op: "delete", Name: "beta"})

	restored := newBucket()
	for _, entry := range [][]byte{walAlpha, walBeta, walGamma, walDeleteBeta} {
		require.NoError(t, restored.Replay(entry))
	}

	// alpha, gamma는 존재하고 beta는 없어야 한다
	for name, shouldExist := range map[string]bool{"alpha": true, "beta": false, "gamma": true} {
		resp, _, _ := restored.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.Equal(t, shouldExist, resp.Found, "bucket %q", name)
	}
}

func TestBucketActor_SnapshotRestore(t *testing.T) {
	// Snapshot/Restore는 checkpoint 기반 빠른 복구를 가능하게 한다.
	// 전체 WAL replay 없이 최신 상태에서 바로 시작할 수 있다.
	original := newBucket()
	original.Receive(nil, BucketRequest{Op: "create", Name: "alpha", Region: "us-east-1"})
	original.Receive(nil, BucketRequest{Op: "create", Name: "beta", Region: "eu-west-1"})

	snap, err := original.Snapshot()
	require.NoError(t, err)

	restored := newBucket()
	require.NoError(t, restored.Restore(snap))

	for _, name := range []string{"alpha", "beta"} {
		resp, _, _ := restored.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.True(t, resp.Found, "bucket %q should exist after restore", name)
	}
	require.EqualValues(t, 2, restored.KeyCount())
}

func TestBucketActor_Split(t *testing.T) {
	// Split은 트래픽/key 수 초과 시 파티션을 둘로 나누는 핵심 기능.
	// splitKey 기준으로 상위 절반은 새 파티션(upper)으로, 하위 절반은 현재 파티션에 남는다.
	a := newBucket()
	a.Receive(nil, BucketRequest{Op: "create", Name: "apple", Region: "us-east-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "banana", Region: "us-east-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "cherry", Region: "eu-west-1"})
	a.Receive(nil, BucketRequest{Op: "create", Name: "date", Region: "eu-west-1"})

	upperData, err := a.Split("cherry")
	require.NoError(t, err)

	upper := newBucket()
	require.NoError(t, upper.Restore(upperData))

	// "cherry", "date" → 상위 파티션
	for _, name := range []string{"cherry", "date"} {
		resp, _, _ := upper.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.True(t, resp.Found, "bucket %q should be in upper partition", name)
	}
	// "apple", "banana" → 하위 파티션(현재 actor)에 남아야 한다
	for _, name := range []string{"apple", "banana"} {
		resp, _, _ := a.Receive(nil, BucketRequest{Op: "get", Name: name})
		require.True(t, resp.Found, "bucket %q should remain in lower partition", name)
	}
	// split 후 중복 없음
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
	// WAL 항목의 직렬화 형식이 올바른지 확인한다.
	// Replay가 다른 인스턴스에서도 동일하게 동작하려면 형식이 안정적이어야 한다.
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
	// Replay 시 CreatedAt이 원본 시각으로 복원되어야 한다.
	// 새로운 time.Now()가 아닌 WAL에 기록된 시각을 사용해야 한다.
	a := newBucket()
	_, walEntry, _ := a.Receive(nil, BucketRequest{Op: "create", Name: "photos", Region: "us-east-1"})
	originalTime := a.buckets["photos"].CreatedAt

	time.Sleep(10 * time.Millisecond)

	restored := newBucket()
	require.NoError(t, restored.Replay(walEntry))
	require.True(t, restored.buckets["photos"].CreatedAt.Equal(originalTime),
		"CreatedAt should be preserved from WAL, not regenerated")
}
