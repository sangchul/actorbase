package rebalance

import (
	"context"
	"errors"
	"testing"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/provider"
)

// adjacentEntries builds two adjacent partitions on the same node.
func adjacentEntries() []domain.RouteEntry {
	return []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "m", "node1", "node1:9000", domain.PartitionStatusActive),
		makeEntry("upper", "kv", "m", "z", "node1", "node1:9000", domain.PartitionStatusActive),
	}
}

func TestMerger_Merge_Success(t *testing.T) {
	store := newMockRoutingStore(makeRT(1, adjacentEntries()))
	ctrl := &mockPSController{}
	factory := newMockPSClientFactory(ctrl)

	m := NewMerger(store, factory)
	if err := m.Merge(context.Background(), "kv", "lower", "upper"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ctrl.mergeCalled {
		t.Error("ExecuteMerge was not called")
	}

	rt, _ := store.Load(context.Background())
	if len(rt.Entries()) != 1 {
		t.Fatalf("expected 1 entry after merge, got %d", len(rt.Entries()))
	}
	lower, ok := rt.LookupByPartition("lower")
	if !ok {
		t.Fatal("lower partition not found after merge")
	}
	if lower.Partition.KeyRange.End != "z" {
		t.Errorf("lower end = %q, want %q", lower.Partition.KeyRange.End, "z")
	}
}

func TestMerger_Merge_NotAdjacent(t *testing.T) {
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "k", "node1", "node1:9000", domain.PartitionStatusActive),
		makeEntry("upper", "kv", "m", "z", "node1", "node1:9000", domain.PartitionStatusActive),
	}
	store := newMockRoutingStore(makeRT(1, entries))
	factory := newMockPSClientFactory(&mockPSController{})

	m := NewMerger(store, factory)
	err := m.Merge(context.Background(), "kv", "lower", "upper")
	if err == nil {
		t.Fatal("expected error for non-adjacent partitions")
	}
}

func TestMerger_Merge_DifferentNodes(t *testing.T) {
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "m", "node1", "node1:9000", domain.PartitionStatusActive),
		makeEntry("upper", "kv", "m", "z", "node2", "node2:9000", domain.PartitionStatusActive),
	}
	store := newMockRoutingStore(makeRT(1, entries))
	factory := newMockPSClientFactory(&mockPSController{})

	m := NewMerger(store, factory)
	err := m.Merge(context.Background(), "kv", "lower", "upper")
	if err == nil {
		t.Fatal("expected error for partitions on different nodes")
	}
}

func TestMerger_Merge_RPCFailure_RevertRouting(t *testing.T) {
	store := newMockRoutingStore(makeRT(1, adjacentEntries()))
	ctrl := &mockPSController{executeMergeErr: errors.New("rpc error")}
	factory := newMockPSClientFactory(ctrl)

	m := NewMerger(store, factory)
	err := m.Merge(context.Background(), "kv", "lower", "upper")
	if err == nil {
		t.Fatal("expected error")
	}
	// Both partitions should be restored.
	rt, _ := store.Load(context.Background())
	if len(rt.Entries()) != 2 {
		t.Errorf("expected routing reverted to 2 entries, got %d", len(rt.Entries()))
	}
	for _, e := range rt.Entries() {
		if e.PartitionStatus != domain.PartitionStatusActive {
			t.Errorf("entry %q status should be Active after revert", e.Partition.ID)
		}
	}
}

// ─── ResumeMerge ──────────────────────────────────────────────────────────────

func TestMerger_ResumeMerge_UpperAlreadyMerged(t *testing.T) {
	// Both Draining — PM crashed after setting draining but ExecuteMerge already ran.
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "m", "node1", "node1:9000", domain.PartitionStatusDraining),
		makeEntry("upper", "kv", "m", "z", "node1", "node1:9000", domain.PartitionStatusDraining),
	}
	store := newMockRoutingStore(makeRT(2, entries))
	// PS responds ErrNotFound for upper — already merged.
	ctrl := &mockPSController{executeMergeErr: provider.ErrNotFound}
	factory := newMockPSClientFactory(ctrl)

	m := NewMerger(store, factory)
	if err := m.ResumeMerge(context.Background(), "kv", "lower", "upper"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rt, _ := store.Load(context.Background())
	if len(rt.Entries()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(rt.Entries()))
	}
	lower, ok := rt.LookupByPartition("lower")
	if !ok {
		t.Fatal("lower partition not found")
	}
	if lower.Partition.KeyRange.End != "z" {
		t.Errorf("lower end = %q, want %q", lower.Partition.KeyRange.End, "z")
	}
}

func TestMerger_ResumeMerge_UpperAlreadyGone_SkipExecute(t *testing.T) {
	// Upper already removed from routing — ResumeMerge should be a no-op.
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusActive),
	}
	store := newMockRoutingStore(makeRT(3, entries))
	ctrl := &mockPSController{}
	factory := newMockPSClientFactory(ctrl)

	m := NewMerger(store, factory)
	if err := m.ResumeMerge(context.Background(), "kv", "lower", "upper"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctrl.mergeCalled {
		t.Error("ExecuteMerge should not be called when upper is already gone from routing")
	}
}

func TestMerger_ResumeMerge_RPCRealError(t *testing.T) {
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "m", "node1", "node1:9000", domain.PartitionStatusDraining),
		makeEntry("upper", "kv", "m", "z", "node1", "node1:9000", domain.PartitionStatusDraining),
	}
	store := newMockRoutingStore(makeRT(2, entries))
	ctrl := &mockPSController{executeMergeErr: errors.New("timeout")}
	factory := newMockPSClientFactory(ctrl)

	m := NewMerger(store, factory)
	err := m.ResumeMerge(context.Background(), "kv", "lower", "upper")
	if err == nil {
		t.Fatal("expected error for non-gone merge error")
	}
}
