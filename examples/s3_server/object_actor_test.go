package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newObject는 테스트용 빈 objectActor를 생성한다.
func newObject() *objectActor {
	return &objectActor{objects: make(map[string]objectMeta), accessCt: make(map[string]int64)}
}

func TestObjectActor_Put(t *testing.T) {
	a := newObject()

	resp, walEntry, err := a.Receive(nil, ObjectRequest{
		Op: "put", Bucket: "photos", Key: "2024/cat.jpg",
		Size: 1024, ETag: "abc123", StorageClass: "STANDARD",
	})

	require.NoError(t, err)
	require.True(t, resp.Found)
	require.Equal(t, "photos", resp.Bucket)
	require.Equal(t, "2024/cat.jpg", resp.Key)
	require.Equal(t, int64(1024), resp.Size)
	require.Equal(t, "abc123", resp.ETag)
	require.Equal(t, "STANDARD", resp.StorageClass)
	require.False(t, resp.LastModified.IsZero())
	require.NotNil(t, walEntry, "put should produce a WAL entry")
}

func TestObjectActor_Get(t *testing.T) {
	a := newObject()
	a.Receive(nil, ObjectRequest{
		Op: "put", Bucket: "photos", Key: "2024/cat.jpg",
		Size: 2048, ETag: "def456", StorageClass: "STANDARD_IA",
	})

	t.Run("existing", func(t *testing.T) {
		resp, walEntry, err := a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "2024/cat.jpg"})
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, int64(2048), resp.Size)
		require.Equal(t, "def456", resp.ETag)
		require.Nil(t, walEntry, "get should not produce a WAL entry (read-only)")
	})

	t.Run("not found", func(t *testing.T) {
		resp, _, err := a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "2024/dog.jpg"})
		require.NoError(t, err)
		require.False(t, resp.Found)
	})
}

func TestObjectActor_Delete(t *testing.T) {
	a := newObject()
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "2024/cat.jpg", Size: 1024, ETag: "abc"})

	_, walEntry, err := a.Receive(nil, ObjectRequest{Op: "delete", Bucket: "photos", Key: "2024/cat.jpg"})
	require.NoError(t, err)
	require.NotNil(t, walEntry, "delete should produce a WAL entry")

	resp, _, _ := a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "2024/cat.jpg"})
	require.False(t, resp.Found, "object should not be found after delete")
}

func TestObjectActor_UnknownOp(t *testing.T) {
	a := newObject()
	_, _, err := a.Receive(nil, ObjectRequest{Op: "copy"})
	require.Error(t, err)
}

func TestObjectActor_WALReplay(t *testing.T) {
	// WAL replay로 원본 상태를 재현한다.
	// put과 delete가 섞인 WAL 시퀀스를 올바르게 처리해야 한다.
	a := newObject()
	_, walCat, _ := a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 100, ETag: "e1"})
	_, walDog, _ := a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "dog.jpg", Size: 200, ETag: "e2"})
	_, walBird, _ := a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "bird.jpg", Size: 300, ETag: "e3"})
	_, walDeleteDog, _ := a.Receive(nil, ObjectRequest{Op: "delete", Bucket: "photos", Key: "dog.jpg"})

	restored := newObject()
	for _, entry := range [][]byte{walCat, walDog, walBird, walDeleteDog} {
		require.NoError(t, restored.Replay(entry))
	}

	for key, shouldExist := range map[string]bool{"cat.jpg": true, "dog.jpg": false, "bird.jpg": true} {
		resp, _, _ := restored.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: key})
		require.Equal(t, shouldExist, resp.Found, "object %q", key)
	}
}

func TestObjectActor_SnapshotRestore(t *testing.T) {
	// checkpoint에서 복원한 actor는 원본과 동일한 상태를 가져야 한다.
	original := newObject()
	original.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 100, ETag: "e1", StorageClass: "STANDARD"})
	original.Receive(nil, ObjectRequest{Op: "put", Bucket: "videos", Key: "movie.mp4", Size: 99999, ETag: "e2", StorageClass: "GLACIER"})

	snap, err := original.Snapshot()
	require.NoError(t, err)

	restored := newObject()
	require.NoError(t, restored.Restore(snap))

	for _, tc := range []struct{ bucket, key, storageClass string }{
		{"photos", "cat.jpg", "STANDARD"},
		{"videos", "movie.mp4", "GLACIER"},
	} {
		resp, _, _ := restored.Receive(nil, ObjectRequest{Op: "get", Bucket: tc.bucket, Key: tc.key})
		require.True(t, resp.Found, "%s/%s should exist after restore", tc.bucket, tc.key)
		require.Equal(t, tc.storageClass, resp.StorageClass)
	}
	require.EqualValues(t, 2, restored.KeyCount())
}

func TestObjectActor_Split(t *testing.T) {
	// routing key는 "{bucket}/{key}" 형태. splitKey도 같은 형식으로 동작한다.
	a := newObject()
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "alpha", Key: "obj1", Size: 1, ETag: "e1"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "alpha", Key: "obj2", Size: 2, ETag: "e2"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "beta", Key: "obj1", Size: 3, ETag: "e3"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "gamma", Key: "obj1", Size: 4, ETag: "e4"})

	upperData, err := a.Split("beta/")
	require.NoError(t, err)

	upper := newObject()
	require.NoError(t, upper.Restore(upperData))

	// 상위 파티션: "beta/obj1", "gamma/obj1"
	for _, tc := range []struct{ bucket, key string }{{"beta", "obj1"}, {"gamma", "obj1"}} {
		resp, _, _ := upper.Receive(nil, ObjectRequest{Op: "get", Bucket: tc.bucket, Key: tc.key})
		require.True(t, resp.Found, "%s/%s should be in upper partition", tc.bucket, tc.key)
	}
	// 하위 파티션: "alpha/obj1", "alpha/obj2"
	for _, tc := range []struct{ bucket, key string }{{"alpha", "obj1"}, {"alpha", "obj2"}} {
		resp, _, _ := a.Receive(nil, ObjectRequest{Op: "get", Bucket: tc.bucket, Key: tc.key})
		require.True(t, resp.Found, "%s/%s should remain in lower partition", tc.bucket, tc.key)
	}
	// split 후 중복 없음
	for _, tc := range []struct{ bucket, key string }{{"beta", "obj1"}, {"gamma", "obj1"}} {
		resp, _, _ := a.Receive(nil, ObjectRequest{Op: "get", Bucket: tc.bucket, Key: tc.key})
		require.False(t, resp.Found, "%s/%s should have been moved to upper partition", tc.bucket, tc.key)
	}
}

func TestObjectActor_KeyCount(t *testing.T) {
	a := newObject()
	require.EqualValues(t, 0, a.KeyCount())

	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 1, ETag: "e1"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "dog.jpg", Size: 2, ETag: "e2"})
	require.EqualValues(t, 2, a.KeyCount())

	a.Receive(nil, ObjectRequest{Op: "delete", Bucket: "photos", Key: "cat.jpg"})
	require.EqualValues(t, 1, a.KeyCount())
}

func TestObjectActor_WALEntryFormat(t *testing.T) {
	// WAL 항목의 routing key가 "{bucket}/{key}" 형태로 올바르게 직렬화되는지 확인한다.
	a := newObject()
	_, walEntry, _ := a.Receive(nil, ObjectRequest{
		Op: "put", Bucket: "photos", Key: "cat.jpg",
		Size: 512, ETag: "abc", StorageClass: "STANDARD",
	})

	var op objectWALOp
	require.NoError(t, json.Unmarshal(walEntry, &op))
	require.Equal(t, "photos/cat.jpg", op.ObjKey)
	require.Equal(t, int64(512), op.Meta.Size)
	require.Equal(t, "abc", op.Meta.ETag)
}

func TestObjectActor_LastModifiedPreservedOnReplay(t *testing.T) {
	// Replay 시 LastModified가 원본 시각으로 복원되어야 한다.
	a := newObject()
	_, walEntry, _ := a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 1, ETag: "e1"})
	originalTime := a.objects["photos/cat.jpg"].LastModified

	time.Sleep(10 * time.Millisecond)

	restored := newObject()
	require.NoError(t, restored.Replay(walEntry))
	require.True(t, restored.objects["photos/cat.jpg"].LastModified.Equal(originalTime),
		"LastModified should be preserved from WAL, not regenerated")
}

func TestObjectActor_SplitHint(t *testing.T) {
	// SplitHint는 가장 많이 접근된 routing key를 반환한다.
	// 이 key를 기준으로 split하면 hotspot이 상위 파티션으로 분리된다.
	a := newObject()

	t.Run("empty actor returns empty hint", func(t *testing.T) {
		require.Empty(t, a.SplitHint())
	})

	// cat.jpg를 5회, dog.jpg를 2회, bird.jpg를 1회 접근
	for i := 0; i < 5; i++ {
		a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "cat.jpg"})
	}
	for i := 0; i < 2; i++ {
		a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "dog.jpg"})
	}
	a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "bird.jpg"})

	t.Run("returns hottest key", func(t *testing.T) {
		hint := a.SplitHint()
		require.Equal(t, "photos/cat.jpg", hint, "가장 많이 접근된 key가 split 위치로 제안되어야 한다")
	})

	t.Run("split at hint separates hotspot", func(t *testing.T) {
		// hotspot key("photos/cat.jpg")를 기준으로 split하면
		// "photos/cat.jpg" 이상의 key들이 상위 파티션으로 이동한다.
		a2 := newObject()
		a2.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "bird.jpg", Size: 1, ETag: "e1"})
		a2.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 2, ETag: "e2"})
		a2.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "dog.jpg", Size: 3, ETag: "e3"})
		for i := 0; i < 10; i++ {
			a2.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "cat.jpg"})
		}

		hint := a2.SplitHint()
		upperData, err := a2.Split(hint)
		require.NoError(t, err)

		upper := newObject()
		require.NoError(t, upper.Restore(upperData))

		// hotspot("cat.jpg")과 그 이후 key("dog.jpg")는 상위 파티션으로 이동
		for _, key := range []string{"cat.jpg", "dog.jpg"} {
			resp, _, _ := upper.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: key})
			require.True(t, resp.Found, "photos/%s should be in upper partition", key)
		}
		// "bird.jpg"는 하위 파티션에 남아야 한다
		resp, _, _ := a2.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "bird.jpg"})
		require.True(t, resp.Found, "photos/bird.jpg should remain in lower partition")
	})
}

func TestObjectActor_PutOverwrite(t *testing.T) {
	// 동일 key에 put을 두 번 하면 최신 값으로 덮어써야 한다.
	a := newObject()
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 100, ETag: "v1"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 200, ETag: "v2"})

	resp, _, _ := a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "cat.jpg"})
	require.Equal(t, int64(200), resp.Size)
	require.Equal(t, "v2", resp.ETag)
	require.EqualValues(t, 1, a.KeyCount(), "overwrite should not increase KeyCount")
}
