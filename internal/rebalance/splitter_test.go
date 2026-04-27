package rebalance

import (
	"context"
	"errors"
	"testing"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
)

func TestSplitter_Split_Success(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "addr1", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}, map[string]string{"node1": "addr1"}))
	ctrl := &mockPSController{executeSplitKey: "m"}
	factory := newMockPSClientFactory(ctrl)

	s := NewSplitter(store, factory)
	newID, err := s.Split(context.Background(), "kv", "p1", "m", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newID == "" {
		t.Fatal("expected a new partition ID")
	}

	// Routing table should now have two entries.
	rt, _ := store.Load(context.Background())
	if len(rt.Entries()) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(rt.Entries()))
	}
	// Lower half ends at split key; upper half starts at split key.
	lower, ok := rt.LookupByPartition("p1")
	if !ok {
		t.Fatal("lower partition p1 not found")
	}
	if lower.Partition.KeyRange.End != "m" {
		t.Errorf("lower end = %q, want %q", lower.Partition.KeyRange.End, "m")
	}
	upper, ok := rt.LookupByPartition(newID)
	if !ok {
		t.Fatal("upper partition not found")
	}
	if upper.Partition.KeyRange.Start != "m" {
		t.Errorf("upper start = %q, want %q", upper.Partition.KeyRange.Start, "m")
	}
}

func TestSplitter_Split_PresetNewPartitionID(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "addr1", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}, map[string]string{"node1": "addr1"}))
	ctrl := &mockPSController{executeSplitKey: "m"}
	factory := newMockPSClientFactory(ctrl)

	s := NewSplitter(store, factory)
	// Provide a pre-generated ID for idempotent resume.
	presetID := "preset-uuid-1234"
	newID, err := s.Split(context.Background(), "kv", "p1", "m", presetID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newID != presetID {
		t.Errorf("newID = %q, want %q", newID, presetID)
	}
}

func TestSplitter_Split_PartitionNotFound(t *testing.T) {
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{}))
	factory := newMockPSClientFactory(&mockPSController{})

	s := NewSplitter(store, factory)
	_, err := s.Split(context.Background(), "kv", "missing", "", "")
	if err == nil {
		t.Fatal("expected error for missing partition")
	}
}

func TestSplitter_Split_ActorTypeMismatch(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "addr1", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
	factory := newMockPSClientFactory(&mockPSController{})

	s := NewSplitter(store, factory)
	_, err := s.Split(context.Background(), "other", "p1", "", "")
	if err == nil {
		t.Fatal("expected error for actor type mismatch")
	}
}

func TestSplitter_Split_RPCFailure(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "addr1", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
	rpcErr := errors.New("network error")
	ctrl := &mockPSController{executeSplitErr: rpcErr}
	factory := newMockPSClientFactory(ctrl)

	s := NewSplitter(store, factory)
	_, err := s.Split(context.Background(), "kv", "p1", "", "")
	if err == nil {
		t.Fatal("expected error on RPC failure")
	}
	// Routing table should remain unchanged (1 entry).
	rt, _ := store.Load(context.Background())
	if len(rt.Entries()) != 1 {
		t.Errorf("expected routing table unchanged, got %d entries", len(rt.Entries()))
	}
}

func TestSplitter_Split_ConnectionFailure(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "addr1", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
	factory := &mockPSClientFactory{err: errors.New("conn refused"), byAddr: make(map[string]transport.PSController)}

	s := NewSplitter(store, factory)
	_, err := s.Split(context.Background(), "kv", "p1", "", "")
	if err == nil {
		t.Fatal("expected error on connection failure")
	}
}

// ─── Epoch invariant ──────────────────────────────────────────────────────────

func TestSplitter_Split_EpochInvariant(t *testing.T) {
	// p1 (to be split) was last assigned at RT version 2.
	// bystander lives on a different node and must not have its epoch changed.
	p1 := makeEntry("p1", "kv", "a", "m", "node1", "addr1", domain.PartitionStatusActive)
	p1.Epoch = 2
	bystander := makeEntry("p2", "kv", "m", "z", "node2", "addr2", domain.PartitionStatusActive)
	bystander.Epoch = 1

	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{p1, bystander},
		map[string]string{"node1": "addr1", "node2": "addr2"}))
	ctrl := &mockPSController{executeSplitKey: "g"}
	factory := newMockPSClientFactory(ctrl)

	s := NewSplitter(store, factory)
	newID, err := s.Split(context.Background(), "kv", "p1", "g", "p1-upper")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resultRT, _ := store.Load(context.Background())
	wantEpoch := uint64(3) // rt.Version()+1 = 2+1

	lower, _ := resultRT.LookupByPartition("p1")
	if lower.Epoch != wantEpoch {
		t.Errorf("lower epoch = %d, want %d", lower.Epoch, wantEpoch)
	}
	upper, ok := resultRT.LookupByPartition(newID)
	if !ok {
		t.Fatalf("upper partition %q not found", newID)
	}
	if upper.Epoch != wantEpoch {
		t.Errorf("upper epoch = %d, want %d", upper.Epoch, wantEpoch)
	}
	// Bystander epoch must not change — this is the core false-rejection-prevention guarantee.
	by, _ := resultRT.LookupByPartition("p2")
	if by.Epoch != 1 {
		t.Errorf("bystander epoch = %d, want 1 (unchanged entry must keep old epoch)", by.Epoch)
	}
}
