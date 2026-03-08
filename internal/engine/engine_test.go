package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oomymy/actorbase/provider"
)

// ── 테스트용 Actor 구현 ─────────────────────────────────────────────────────

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

func (a *kvActor) Snapshot() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Marshal(a.data)
}

func (a *kvActor) Restore(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Unmarshal(data, &a.data)
}

func (a *kvActor) Split(splitKey string) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	upper := make(map[string]string)
	for k, v := range a.data {
		if k >= splitKey {
			upper[k] = v
			delete(a.data, k)
		}
	}
	return json.Marshal(upper)
}

// ── 테스트용 Store 구현 ─────────────────────────────────────────────────────

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

// ── 헬퍼 ────────────────────────────────────────────────────────────────────

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

// ── 테스트 ───────────────────────────────────────────────────────────────────

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

	// 모든 키가 존재하는지 확인
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

	// 첫 번째 ActorHost: 데이터 쓰고 checkpoint
	h1 := NewActorHost[kvReq, kvResp](cfg)
	put(t, h1, "p1", "foo", "bar")
	time.Sleep(20 * time.Millisecond) // WAL flush 대기
	if err := h1.Checkpoint(ctx, "p1"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	h1.EvictAll(ctx) //nolint:errcheck

	// 두 번째 ActorHost: checkpoint에서 복원
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

	// evict 후 CheckpointStore에 데이터가 저장되어 있어야 한다
	raw, _ := cp.Load(ctx, "p1")
	if len(raw) < 8 {
		t.Fatal("expected checkpoint data after evict")
	}

	// 새 호스트로 복원 확인
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

	// 쓰기 후 WAL은 있지만 checkpoint는 없는 상태로 evict
	h1 := NewActorHost[kvReq, kvResp](cfg)
	put(t, h1, "p1", "key", "value")
	time.Sleep(20 * time.Millisecond) // WAL flush 대기

	// checkpoint 없이 Evict → WALStore에는 남아 있음
	// (실제로 Evict는 checkpoint를 하므로 이 테스트는 WAL replay 자체를 검증하기 위해
	//  CheckpointStore 없이 WAL만 있는 경우를 수동으로 구성한다)
	h1.EvictAll(ctx) //nolint:errcheck

	// WAL만 있고 checkpoint는 없는 상태 시뮬레이션:
	// cp를 비우고 WAL은 유지
	delete(cp.data, "p1") // checkpoint 제거

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

	// "a", "b", "c", "d" 삽입
	for _, k := range []string{"a", "b", "c", "d"} {
		put(t, h, "p1", k, k+"-val")
	}
	time.Sleep(20 * time.Millisecond)

	// "c" 기준으로 split: p1=[a,b], p2=[c,d]
	if err := h.Split(ctx, "p1", "c", "p2"); err != nil {
		t.Fatalf("Split: %v", err)
	}

	// 하위 파티션 검증
	if v := get(t, h, "p1", "a"); v != "a-val" {
		t.Errorf("p1[a] = %q, want a-val", v)
	}
	if v := get(t, h, "p1", "b"); v != "b-val" {
		t.Errorf("p1[b] = %q, want b-val", v)
	}

	// 상위 파티션 검증
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

type panicActor struct{}

func (a *panicActor) Receive(_ provider.Context, _ kvReq) (kvResp, []byte, error) {
	panic("intentional panic")
}
func (a *panicActor) Replay(_ []byte) error                         { return nil }
func (a *panicActor) Snapshot() ([]byte, error)                     { return nil, nil }
func (a *panicActor) Restore(_ []byte) error                        { return nil }
func (a *panicActor) Split(_ string) ([]byte, error)                { return nil, nil }
