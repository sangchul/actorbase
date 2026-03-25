package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sangchul/actorbase/provider"
)

// ── Actor implementation for tests ─────────────────────────────────────────

type kvReq struct {
	Op    string // "get" | "put" | "delete"
	Key   string
	Value string
}

type kvResp struct {
	Value string
}

type kvActor struct {
	mu   sync.Mutex
	data map[string]string
}

func newKVActor(_ string) provider.Actor[kvReq, kvResp] {
	return &kvActor{data: make(map[string]string)}
}

func (a *kvActor) Receive(_ provider.Context, req kvReq) (kvResp, []byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch req.Op {
	case "get":
		v, ok := a.data[req.Key]
		if !ok {
			return kvResp{}, nil, provider.ErrNotFound
		}
		return kvResp{Value: v}, nil, nil
	case "put":
		a.data[req.Key] = req.Value
		entry, _ := json.Marshal(req)
		return kvResp{}, entry, nil
	case "delete":
		delete(a.data, req.Key)
		entry, _ := json.Marshal(req)
		return kvResp{}, entry, nil
	}
	return kvResp{}, nil, fmt.Errorf("unknown op: %s", req.Op)
}

func (a *kvActor) Replay(entry []byte) error {
	var req kvReq
	if err := json.Unmarshal(entry, &req); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	switch req.Op {
	case "put":
		a.data[req.Key] = req.Value
	case "delete":
		delete(a.data, req.Key)
	}
	return nil
}

func (a *kvActor) Export(splitKey string) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if splitKey == "" {
		return json.Marshal(a.data)
	}
	upper := make(map[string]string)
	for k, v := range a.data {
		if k >= splitKey {
			upper[k] = v
			delete(a.data, k)
		}
	}
	return json.Marshal(upper)
}

func (a *kvActor) Import(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var incoming map[string]string
	if err := json.Unmarshal(data, &incoming); err != nil {
		return err
	}
	for k, v := range incoming {
		a.data[k] = v
	}
	return nil
}

// ── Store implementation for tests ─────────────────────────────────────────

type memWALStore struct {
	mu      sync.Mutex
	entries map[string][]provider.WALEntry
	nextLSN map[string]uint64
}

func newMemWALStore() *memWALStore {
	return &memWALStore{
		entries: make(map[string][]provider.WALEntry),
		nextLSN: make(map[string]uint64),
	}
}

func (s *memWALStore) AppendBatch(_ context.Context, partitionID string, data [][]byte) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lsns := make([]uint64, len(data))
	for i, d := range data {
		s.nextLSN[partitionID]++
		lsn := s.nextLSN[partitionID]
		s.entries[partitionID] = append(s.entries[partitionID], provider.WALEntry{LSN: lsn, Data: d})
		lsns[i] = lsn
	}
	return lsns, nil
}

func (s *memWALStore) ReadFrom(_ context.Context, partitionID string, fromLSN uint64) ([]provider.WALEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []provider.WALEntry
	for _, e := range s.entries[partitionID] {
		if e.LSN >= fromLSN {
			result = append(result, e)
		}
	}
	return result, nil
}

func (s *memWALStore) TrimBefore(_ context.Context, partitionID string, lsn uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var kept []provider.WALEntry
	for _, e := range s.entries[partitionID] {
		if e.LSN >= lsn {
			kept = append(kept, e)
		}
	}
	s.entries[partitionID] = kept
	return nil
}

type memCheckpointStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemCheckpointStore() *memCheckpointStore {
	return &memCheckpointStore{data: make(map[string][]byte)}
}

func (s *memCheckpointStore) Save(_ context.Context, partitionID string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[partitionID] = append([]byte(nil), data...)
	return nil
}

func (s *memCheckpointStore) Load(_ context.Context, partitionID string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[partitionID], nil
}

func (s *memCheckpointStore) Delete(_ context.Context, partitionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, partitionID)
	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func newTestHost(t *testing.T) *ActorHost[kvReq, kvResp] {
	t.Helper()
	return NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})
}

func put(t *testing.T, h *ActorHost[kvReq, kvResp], partID, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := h.Send(ctx, partID, kvReq{Op: "put", Key: key, Value: value}); err != nil {
		t.Fatalf("put %s=%s: %v", key, value, err)
	}
}

func get(t *testing.T, h *ActorHost[kvReq, kvResp], partID, key string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := h.Send(ctx, partID, kvReq{Op: "get", Key: key})
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return resp.Value
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestActorHost_Send_BasicPutGet(t *testing.T) {
	h := newTestHost(t)
	ctx := context.Background()

	put(t, h, "p1", "hello", "world")

	v := get(t, h, "p1", "hello")
	if v != "world" {
		t.Errorf("get = %q, want world", v)
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_Send_ConcurrentRequests(t *testing.T) {
	h := newTestHost(t)
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			put(t, h, "p1", key, fmt.Sprintf("val-%d", i))
		}(i)
	}
	wg.Wait()

	// verify all keys exist
	for i := range n {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)
		if v := get(t, h, "p1", key); v != want {
			t.Errorf("key %s: got %q, want %q", key, v, want)
		}
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_Checkpoint_RestoresState(t *testing.T) {
	wal := newMemWALStore()
	cp := newMemCheckpointStore()

	cfg := Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        wal,
		CheckpointStore: cp,
		FlushInterval:   5 * time.Millisecond,
	}

	ctx := context.Background()

	// first ActorHost: write data and checkpoint
	h1 := NewActorHost[kvReq, kvResp](cfg)
	put(t, h1, "p1", "foo", "bar")
	time.Sleep(20 * time.Millisecond) // wait for WAL flush
	if err := h1.Checkpoint(ctx, "p1"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	h1.EvictAll(ctx) //nolint:errcheck

	// second ActorHost: restore from checkpoint
	h2 := NewActorHost[kvReq, kvResp](cfg)
	if v := get(t, h2, "p1", "foo"); v != "bar" {
		t.Errorf("after restore: got %q, want bar", v)
	}
	h2.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_Evict_CheckpointsBeforeRemoval(t *testing.T) {
	wal := newMemWALStore()
	cp := newMemCheckpointStore()

	cfg := Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        wal,
		CheckpointStore: cp,
		FlushInterval:   5 * time.Millisecond,
	}

	ctx := context.Background()

	h1 := NewActorHost[kvReq, kvResp](cfg)
	put(t, h1, "p1", "k", "v")
	time.Sleep(20 * time.Millisecond)
	if err := h1.Evict(ctx, "p1"); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	// after evict, checkpoint data should be present in the CheckpointStore
	raw, _ := cp.Load(ctx, "p1")
	if len(raw) < 8 {
		t.Fatal("expected checkpoint data after evict")
	}

	// verify restoration on a new host
	h2 := NewActorHost[kvReq, kvResp](cfg)
	if v := get(t, h2, "p1", "k"); v != "v" {
		t.Errorf("after evict+restore: got %q, want v", v)
	}
	h2.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_WALReplay_OnActivation(t *testing.T) {
	wal := newMemWALStore()
	cp := newMemCheckpointStore()

	cfg := Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        wal,
		CheckpointStore: cp,
		FlushInterval:   5 * time.Millisecond,
	}

	ctx := context.Background()

	// write data so WAL exists, then evict without retaining the checkpoint
	h1 := NewActorHost[kvReq, kvResp](cfg)
	put(t, h1, "p1", "key", "value")
	time.Sleep(20 * time.Millisecond) // wait for WAL flush

	// Evict saves a checkpoint; to test WAL-only replay we manually clear
	// the checkpoint after eviction, leaving only the WAL entries.
	h1.EvictAll(ctx) //nolint:errcheck

	// simulate state where only WAL exists and checkpoint has been removed:
	// clear the checkpoint store while keeping the WAL intact
	delete(cp.data, "p1") // remove checkpoint

	h2 := NewActorHost[kvReq, kvResp](cfg)
	if v := get(t, h2, "p1", "key"); v != "value" {
		t.Errorf("WAL replay: got %q, want value", v)
	}
	h2.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_Split(t *testing.T) {
	wal := newMemWALStore()
	cp := newMemCheckpointStore()

	cfg := Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        wal,
		CheckpointStore: cp,
		FlushInterval:   5 * time.Millisecond,
	}

	ctx := context.Background()
	h := NewActorHost[kvReq, kvResp](cfg)

	// insert "a", "b", "c", "d"
	for _, k := range []string{"a", "b", "c", "d"} {
		put(t, h, "p1", k, k+"-val")
	}
	time.Sleep(20 * time.Millisecond)

	// split at "c": p1=[a,b], p2=[c,d]
	if _, err := h.Split(ctx, "p1", "c", "", "", "p2"); err != nil {
		t.Fatalf("Split: %v", err)
	}

	// verify lower partition
	if v := get(t, h, "p1", "a"); v != "a-val" {
		t.Errorf("p1[a] = %q, want a-val", v)
	}
	if v := get(t, h, "p1", "b"); v != "b-val" {
		t.Errorf("p1[b] = %q, want b-val", v)
	}

	// verify upper partition
	if v := get(t, h, "p2", "c"); v != "c-val" {
		t.Errorf("p2[c] = %q, want c-val", v)
	}
	if v := get(t, h, "p2", "d"); v != "d-val" {
		t.Errorf("p2[d] = %q, want d-val", v)
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_IdleActors(t *testing.T) {
	h := newTestHost(t)
	ctx := context.Background()

	put(t, h, "p1", "k", "v")
	put(t, h, "p2", "k", "v")

	time.Sleep(50 * time.Millisecond)

	idle := h.IdleActors(time.Now().Add(-20 * time.Millisecond))
	if len(idle) != 2 {
		t.Errorf("expected 2 idle actors, got %d: %v", len(idle), idle)
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_ActivePartitions(t *testing.T) {
	h := newTestHost(t)
	ctx := context.Background()

	put(t, h, "p1", "k", "v")
	put(t, h, "p2", "k", "v")

	active := h.ActivePartitions()
	if len(active) != 2 {
		t.Errorf("expected 2 active partitions, got %d", len(active))
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_PanicActor_ReturnsErrActorPanicked(t *testing.T) {
	panicFactory := func(_ string) provider.Actor[kvReq, kvResp] {
		return &panicActor{}
	}
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         panicFactory,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := h.Send(ctx, "p1", kvReq{Op: "get", Key: "k"})
	if err != provider.ErrActorPanicked {
		t.Errorf("expected ErrActorPanicked, got %v", err)
	}
}

// ── Countable actor ───────────────────────────────────────────────────────────

type countableKVActor struct {
	kvActor
}

func newCountableKVActor(_ string) provider.Actor[kvReq, kvResp] {
	return &countableKVActor{kvActor: kvActor{data: make(map[string]string)}}
}

func (a *countableKVActor) KeyCount() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return int64(len(a.data))
}

// ── SplitHinter actor ─────────────────────────────────────────────────────────

type splitHinterKVActor struct {
	kvActor
	hint string
}

func (a *splitHinterKVActor) SplitHint() string { return a.hint }

// ── GetStats tests ────────────────────────────────────────────────────────────

func TestActorHost_GetStats_NonCountable(t *testing.T) {
	// kvActor does not implement Countable, so KeyCount should be -1
	h := newTestHost(t)
	ctx := context.Background()

	put(t, h, "p1", "k1", "v1")
	put(t, h, "p1", "k2", "v2")
	time.Sleep(20 * time.Millisecond) // wait for WAL flush

	stats := h.GetStats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 partition stat, got %d", len(stats))
	}
	if stats[0].KeyCount != -1 {
		t.Errorf("non-Countable actor: KeyCount = %d, want -1", stats[0].KeyCount)
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_GetStats_Countable(t *testing.T) {
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newCountableKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})
	ctx := context.Background()

	put(t, h, "p1", "k1", "v1")
	put(t, h, "p1", "k2", "v2")
	time.Sleep(20 * time.Millisecond) // wait for WAL flush and KeyCount update

	stats := h.GetStats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 partition stat, got %d", len(stats))
	}
	if stats[0].KeyCount != 2 {
		t.Errorf("Countable actor: KeyCount = %d, want 2", stats[0].KeyCount)
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_GetStats_EvictedActorNotReported(t *testing.T) {
	h := newTestHost(t)
	ctx := context.Background()

	put(t, h, "p1", "k", "v")
	put(t, h, "p2", "k", "v")
	time.Sleep(20 * time.Millisecond)

	if err := h.Evict(ctx, "p1"); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	stats := h.GetStats()
	for _, s := range stats {
		if s.PartitionID == "p1" {
			t.Error("evicted partition p1 should not appear in GetStats()")
		}
	}
	if len(stats) != 1 {
		t.Errorf("expected 1 stat (p2 only), got %d", len(stats))
	}

	h.EvictAll(ctx) //nolint:errcheck
}

// ── Split auto key tests ──────────────────────────────────────────────────────

func TestActorHost_Split_WithSplitHinter(t *testing.T) {
	hintKey := "c"
	factory := func(_ string) provider.Actor[kvReq, kvResp] {
		a := &splitHinterKVActor{hint: hintKey}
		a.data = make(map[string]string)
		return a
	}
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         factory,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})
	ctx := context.Background()

	for _, k := range []string{"a", "b", "c", "d"} {
		put(t, h, "p1", k, k+"-val")
	}
	time.Sleep(20 * time.Millisecond)

	// splitKey="" → delegates to SplitHinter.SplitHint() = "c"
	usedKey, err := h.Split(ctx, "p1", "", "a", "z", "p2")
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if usedKey != hintKey {
		t.Errorf("expected hint key %q, got %q", hintKey, usedKey)
	}

	// p1: a,b (< "c")
	for _, k := range []string{"a", "b"} {
		if v := get(t, h, "p1", k); v != k+"-val" {
			t.Errorf("p1[%s] = %q, want %s-val", k, v, k)
		}
	}
	// p2: c,d (>= "c")
	for _, k := range []string{"c", "d"} {
		if v := get(t, h, "p2", k); v != k+"-val" {
			t.Errorf("p2[%s] = %q, want %s-val", k, v, k)
		}
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_Split_MidpointFallback(t *testing.T) {
	// splitKey="" with no SplitHinter → falls back to KeyRangeMidpoint("a","z") = "m"
	h := newTestHost(t)
	ctx := context.Background()

	for _, k := range []string{"a", "b", "c", "n", "y", "z"} {
		put(t, h, "p1", k, k+"-val")
	}
	time.Sleep(20 * time.Millisecond)

	usedKey, err := h.Split(ctx, "p1", "", "a", "z", "p2")
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if usedKey != "m" {
		t.Errorf("expected midpoint key 'm', got %q", usedKey)
	}

	// p1: a,b,c (< "m")
	for _, k := range []string{"a", "b", "c"} {
		if v := get(t, h, "p1", k); v != k+"-val" {
			t.Errorf("p1[%s] = %q, want %s-val", k, v, k)
		}
	}
	// p2: n,y,z (>= "m")
	for _, k := range []string{"n", "y", "z"} {
		if v := get(t, h, "p2", k); v != k+"-val" {
			t.Errorf("p2[%s] = %q, want %s-val", k, v, k)
		}
	}

	h.EvictAll(ctx) //nolint:errcheck
}

func TestActorHost_Merge(t *testing.T) {
	wal := newMemWALStore()
	cp := newMemCheckpointStore()
	h := NewActorHost(Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        wal,
		CheckpointStore: cp,
		FlushSize:       16,
		FlushInterval:   5 * time.Millisecond,
		MailboxSize:     64,
	})
	ctx := context.Background()

	// lower partition: a, b, c
	for _, k := range []string{"a", "b", "c"} {
		put(t, h, "lower", k, k+"-val")
	}
	// upper partition: x, y, z
	for _, k := range []string{"x", "y", "z"} {
		put(t, h, "upper", k, k+"-val")
	}
	time.Sleep(20 * time.Millisecond) // wait for WAL flush

	// merge: lower absorbs upper
	if err := h.Merge(ctx, "lower", "upper"); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// lower should now contain upper's data
	for _, k := range []string{"a", "b", "c", "x", "y", "z"} {
		if v := get(t, h, "lower", k); v != k+"-val" {
			t.Errorf("lower[%s] = %q, want %s-val", k, v, k)
		}
	}

	// upper has been evicted; sending to it creates a fresh empty actor with no prior data
	_, err := h.Send(ctx, "upper", kvReq{Op: "get", Key: "x"})
	if err != provider.ErrNotFound {
		t.Errorf("expected ErrNotFound for evicted upper, got %v", err)
	}

	h.EvictAll(ctx) //nolint:errcheck

	// after lower is evicted and reactivated, it should be restored from the checkpoint
	h2 := NewActorHost(Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        wal,
		CheckpointStore: cp,
		FlushSize:       16,
		FlushInterval:   5 * time.Millisecond,
		MailboxSize:     64,
	})
	for _, k := range []string{"a", "b", "c", "x", "y", "z"} {
		if v := get(t, h2, "lower", k); v != k+"-val" {
			t.Errorf("after restore: lower[%s] = %q, want %s-val", k, v, k)
		}
	}
	h2.EvictAll(ctx) //nolint:errcheck
}

// ── Panic actor ───────────────────────────────────────────────────────────────

type panicActor struct{}

func (a *panicActor) Receive(_ provider.Context, _ kvReq) (kvResp, []byte, error) {
	panic("intentional panic")
}
func (a *panicActor) Replay(_ []byte) error              { return nil }
func (a *panicActor) Export(_ string) ([]byte, error)    { return nil, nil }
func (a *panicActor) Import(_ []byte) error              { return nil }
