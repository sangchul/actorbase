package engine

import (
	"context"
	"testing"
	"time"
)

func TestEvictionScheduler_EvictsIdleActors(t *testing.T) {
	cp := newMemCheckpointStore()
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: cp,
		FlushInterval:   5 * time.Millisecond,
	})

	put(t, h, "p1", "k", "v")
	put(t, h, "p2", "k", "v")

	// Wait long enough for both actors to become idle.
	time.Sleep(60 * time.Millisecond)

	idleTimeout := 20 * time.Millisecond
	interval := 10 * time.Millisecond
	s := NewEvictionScheduler(h, idleTimeout, interval)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go s.Start(ctx)

	// Allow the scheduler enough cycles to evict idle actors.
	time.Sleep(100 * time.Millisecond)

	active := h.ActivePartitions()
	if len(active) != 0 {
		t.Errorf("expected 0 active partitions after eviction, got %d: %v", len(active), active)
	}
}

func TestEvictionScheduler_DoesNotEvictRecentlyActive(t *testing.T) {
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})

	// Keep p1 busy so it is not idle.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	put(t, h, "p1", "k", "v")

	s := NewEvictionScheduler(h, 200*time.Millisecond, 20*time.Millisecond)
	go s.Start(ctx)

	// Within the idle timeout window, p1 should still be active.
	time.Sleep(50 * time.Millisecond)
	active := h.ActivePartitions()
	found := false
	for _, id := range active {
		if id == "p1" {
			found = true
		}
	}
	if !found {
		t.Error("p1 should still be active within idle timeout")
	}
}

func TestEvictionScheduler_StopsOnContextCancel(t *testing.T) {
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	s := NewEvictionScheduler(h, 10*time.Millisecond, 5*time.Millisecond)

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// scheduler exited cleanly
	case <-time.After(200 * time.Millisecond):
		t.Fatal("EvictionScheduler did not stop after context cancel")
	}
}

func TestCheckpointScheduler_CheckpointsActivePartitions(t *testing.T) {
	cp := newMemCheckpointStore()
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: cp,
		FlushInterval:   5 * time.Millisecond,
	})

	put(t, h, "p1", "k", "v")
	time.Sleep(20 * time.Millisecond) // wait for WAL flush

	// Clear any checkpoint written by Evict (none here, just precaution).
	cp.mu.Lock()
	delete(cp.data, "p1")
	cp.mu.Unlock()

	s := NewCheckpointScheduler(h, 30*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go s.Start(ctx)

	// Allow at least one checkpoint cycle.
	time.Sleep(100 * time.Millisecond)

	raw, err := cp.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if len(raw) == 0 {
		t.Error("expected checkpoint to be written by CheckpointScheduler")
	}
}

func TestCheckpointScheduler_StopsOnContextCancel(t *testing.T) {
	h := NewActorHost[kvReq, kvResp](Config[kvReq, kvResp]{
		Factory:         newKVActor,
		WALStore:        newMemWALStore(),
		CheckpointStore: newMemCheckpointStore(),
		FlushInterval:   5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	s := NewCheckpointScheduler(h, 10*time.Millisecond)

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// scheduler exited cleanly
	case <-time.After(200 * time.Millisecond):
		t.Fatal("CheckpointScheduler did not stop after context cancel")
	}
}
