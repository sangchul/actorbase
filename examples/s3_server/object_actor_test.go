package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newObject creates an empty objectActor for testing.
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
	// WAL replay reproduces the original state.
	// A mixed sequence of put and delete WAL entries must be handled correctly.
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
	// An actor restored from a checkpoint must have the same state as the original.
	original := newObject()
	original.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 100, ETag: "e1", StorageClass: "STANDARD"})
	original.Receive(nil, ObjectRequest{Op: "put", Bucket: "videos", Key: "movie.mp4", Size: 99999, ETag: "e2", StorageClass: "GLACIER"})

	snap, err := original.Export("")
	require.NoError(t, err)

	restored := newObject()
	require.NoError(t, restored.Import(snap))

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
	// routing key is in the form "{bucket}/{key}"; splitKey uses the same format.
	a := newObject()
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "alpha", Key: "obj1", Size: 1, ETag: "e1"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "alpha", Key: "obj2", Size: 2, ETag: "e2"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "beta", Key: "obj1", Size: 3, ETag: "e3"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "gamma", Key: "obj1", Size: 4, ETag: "e4"})

	upperData, err := a.Export("beta/")
	require.NoError(t, err)

	upper := newObject()
	require.NoError(t, upper.Import(upperData))

	// upper partition: "beta/obj1", "gamma/obj1"
	for _, tc := range []struct{ bucket, key string }{{"beta", "obj1"}, {"gamma", "obj1"}} {
		resp, _, _ := upper.Receive(nil, ObjectRequest{Op: "get", Bucket: tc.bucket, Key: tc.key})
		require.True(t, resp.Found, "%s/%s should be in upper partition", tc.bucket, tc.key)
	}
	// lower partition: "alpha/obj1", "alpha/obj2"
	for _, tc := range []struct{ bucket, key string }{{"alpha", "obj1"}, {"alpha", "obj2"}} {
		resp, _, _ := a.Receive(nil, ObjectRequest{Op: "get", Bucket: tc.bucket, Key: tc.key})
		require.True(t, resp.Found, "%s/%s should remain in lower partition", tc.bucket, tc.key)
	}
	// no duplicates after split
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
	// Verifies that the WAL entry's routing key is correctly serialized as "{bucket}/{key}".
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
	// On Replay, LastModified must be restored to the original timestamp.
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
	// SplitHint returns the most-accessed routing key.
	// Splitting at this key moves the hotspot into the upper partition.
	a := newObject()

	t.Run("empty actor returns empty hint", func(t *testing.T) {
		require.Empty(t, a.SplitHint())
	})

	// access cat.jpg 5 times, dog.jpg 2 times, bird.jpg 1 time
	for i := 0; i < 5; i++ {
		a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "cat.jpg"})
	}
	for i := 0; i < 2; i++ {
		a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "dog.jpg"})
	}
	a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "bird.jpg"})

	t.Run("returns hottest key", func(t *testing.T) {
		hint := a.SplitHint()
		require.Equal(t, "photos/cat.jpg", hint, "the most-accessed key should be proposed as the split point")
	})

	t.Run("split at hint separates hotspot", func(t *testing.T) {
		// splitting at the hotspot key ("photos/cat.jpg") moves keys >= that value
		// into the upper partition.
		a2 := newObject()
		a2.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "bird.jpg", Size: 1, ETag: "e1"})
		a2.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 2, ETag: "e2"})
		a2.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "dog.jpg", Size: 3, ETag: "e3"})
		for i := 0; i < 10; i++ {
			a2.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "cat.jpg"})
		}

		hint := a2.SplitHint()
		upperData, err := a2.Export(hint)
		require.NoError(t, err)

		upper := newObject()
		require.NoError(t, upper.Import(upperData))

		// hotspot ("cat.jpg") and subsequent keys ("dog.jpg") move to the upper partition
		for _, key := range []string{"cat.jpg", "dog.jpg"} {
			resp, _, _ := upper.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: key})
			require.True(t, resp.Found, "photos/%s should be in upper partition", key)
		}
		// "bird.jpg" should remain in the lower partition
		resp, _, _ := a2.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "bird.jpg"})
		require.True(t, resp.Found, "photos/bird.jpg should remain in lower partition")
	})
}

func TestObjectActor_PutOverwrite(t *testing.T) {
	// Putting the same key twice should overwrite with the latest value.
	a := newObject()
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 100, ETag: "v1"})
	a.Receive(nil, ObjectRequest{Op: "put", Bucket: "photos", Key: "cat.jpg", Size: 200, ETag: "v2"})

	resp, _, _ := a.Receive(nil, ObjectRequest{Op: "get", Bucket: "photos", Key: "cat.jpg"})
	require.Equal(t, int64(200), resp.Size)
	require.Equal(t, "v2", resp.ETag)
	require.EqualValues(t, 1, a.KeyCount(), "overwrite should not increase KeyCount")
}
