package rebalance

import (
	"context"
	"errors"
	"testing"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	"github.com/sangchul/actorbase/provider"
)

func makeNodes(ids ...string) []domain.NodeInfo {
	nodes := make([]domain.NodeInfo, len(ids))
	for i, id := range ids {
		nodes[i] = domain.NodeInfo{ID: id, Address: id + ":9000", Status: domain.NodeStatusActive}
	}
	return nodes
}

func TestMigrator_Migrate_Success(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}, map[string]string{"node1": "node1:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}
	ctrl := &mockPSController{}
	factory := newMockPSClientFactory(ctrl)

	m := NewMigrator(store, catalog, factory)
	if err := m.Migrate(context.Background(), "kv", "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ctrl.migrateOutCalled {
		t.Error("ExecuteMigrateOut was not called")
	}
	if !ctrl.preparePartitionCalled {
		t.Error("PreparePartition was not called")
	}

	rt, _ := store.Load(context.Background())
	e, _ := rt.LookupByPartition("p1")
	if e.NodeID != "node2" {
		t.Errorf("partition node = %q, want %q", e.NodeID, "node2")
	}
	if e.PartitionStatus != domain.PartitionStatusActive {
		t.Error("partition status should be Active after migrate")
	}
}

func TestMigrator_Migrate_AlreadyDraining(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}
	factory := newMockPSClientFactory(&mockPSController{})

	m := NewMigrator(store, catalog, factory)
	err := m.Migrate(context.Background(), "kv", "p1", "node2")
	if err == nil {
		t.Fatal("expected error for draining partition")
	}
}

func TestMigrator_Migrate_ActorTypeMismatch(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}
	factory := newMockPSClientFactory(&mockPSController{})

	m := NewMigrator(store, catalog, factory)
	err := m.Migrate(context.Background(), "other", "p1", "node2")
	if err == nil {
		t.Fatal("expected error for actor type mismatch")
	}
}

func TestMigrator_Migrate_MigrateOutFailure_RevertRouting(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}, map[string]string{"node1": "node1:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}
	ctrl := &mockPSController{executeMigrateOutErr: errors.New("rpc error")}
	factory := newMockPSClientFactory(ctrl)

	m := NewMigrator(store, catalog, factory)
	err := m.Migrate(context.Background(), "kv", "p1", "node2")
	if err == nil {
		t.Fatal("expected error")
	}
	// Routing must be reverted to Active on source node.
	rt, _ := store.Load(context.Background())
	e, _ := rt.LookupByPartition("p1")
	if e.PartitionStatus != domain.PartitionStatusActive {
		t.Error("routing should be reverted to Active after MigrateOut failure")
	}
	if e.NodeID != "node1" {
		t.Error("routing should remain on source node after MigrateOut failure")
	}
}

// ─── ResumeMigrate ────────────────────────────────────────────────────────────

func TestMigrator_ResumeMigrate_SourceAlreadyEvicted(t *testing.T) {
	// Routing is Draining (PM crashed after MigrateOut but before PreparePartition).
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{entry}, map[string]string{"node1": "node1:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}

	// Source returns ErrNotFound — already evicted.
	sourceCtrl := &mockPSController{executeMigrateOutErr: provider.ErrNotFound}
	targetCtrl := &mockPSController{}
	factory := newMockPSClientFactory(nil)
	factory.byAddr["node1:9000"] = sourceCtrl
	factory.byAddr["node2:9000"] = targetCtrl

	m := NewMigrator(store, catalog, factory)
	if err := m.ResumeMigrate(context.Background(), "kv", "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !targetCtrl.preparePartitionCalled {
		t.Error("PreparePartition was not called on target")
	}
	rt, _ := store.Load(context.Background())
	e, _ := rt.LookupByPartition("p1")
	if e.NodeID != "node2" {
		t.Errorf("partition node = %q, want %q", e.NodeID, "node2")
	}
	if e.PartitionStatus != domain.PartitionStatusActive {
		t.Error("partition should be Active after ResumeMigrate")
	}
}

func TestMigrator_ResumeMigrate_SourceNotOwned(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{entry}, map[string]string{"node1": "node1:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}

	sourceCtrl := &mockPSController{executeMigrateOutErr: provider.ErrPartitionNotOwned}
	targetCtrl := &mockPSController{}
	factory := newMockPSClientFactory(nil)
	factory.byAddr["node1:9000"] = sourceCtrl
	factory.byAddr["node2:9000"] = targetCtrl

	m := NewMigrator(store, catalog, factory)
	if err := m.ResumeMigrate(context.Background(), "kv", "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !targetCtrl.preparePartitionCalled {
		t.Error("PreparePartition was not called on target")
	}
}

func TestMigrator_ResumeMigrate_SourceRealError(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{entry}, map[string]string{"node1": "node1:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}

	// A non-"already done" error must propagate.
	sourceCtrl := &mockPSController{executeMigrateOutErr: errors.New("timeout")}
	factory := newMockPSClientFactory(nil)
	factory.byAddr["node1:9000"] = sourceCtrl
	factory.byAddr["node2:9000"] = &mockPSController{}

	m := NewMigrator(store, catalog, factory)
	err := m.ResumeMigrate(context.Background(), "kv", "p1", "node2")
	if err == nil {
		t.Fatal("expected error for non-gone source error")
	}
}

// ─── Failover ─────────────────────────────────────────────────────────────────

func TestMigrator_Failover_Success(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "dead", "dead:9000", domain.PartitionStatusActive)
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node2")}
	targetCtrl := &mockPSController{}
	factory := &mockPSClientFactory{byAddr: map[string]transport.PSController{
		"node2:9000": targetCtrl,
	}}

	m := NewMigrator(store, catalog, factory)
	if err := m.Failover(context.Background(), "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !targetCtrl.preparePartitionCalled {
		t.Error("PreparePartition was not called on target")
	}
	rt, _ := store.Load(context.Background())
	e, _ := rt.LookupByPartition("p1")
	if e.NodeID != "node2" {
		t.Errorf("partition node = %q, want %q", e.NodeID, "node2")
	}
}

// ─── Epoch invariant ──────────────────────────────────────────────────────────

func TestMigrator_Migrate_EpochInvariant(t *testing.T) {
	p1 := makeEntry("p1", "kv", "a", "m", "node1", "node1:9000", domain.PartitionStatusActive)
	p1.Epoch = 2
	bystander := makeEntry("p2", "kv", "m", "z", "node1", "node1:9000", domain.PartitionStatusActive)
	bystander.Epoch = 1
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{p1, bystander},
		map[string]string{"node1": "node1:9000", "node2": "node2:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}
	ctrl := &mockPSController{}
	factory := newMockPSClientFactory(ctrl)

	m := NewMigrator(store, catalog, factory)
	if err := m.Migrate(context.Background(), "kv", "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resultRT, _ := store.Load(context.Background())
	wantEpoch := uint64(3) // rt.Version()+1 = 2+1

	migrated, _ := resultRT.LookupByPartition("p1")
	if migrated.Epoch != wantEpoch {
		t.Errorf("migrated entry epoch = %d, want %d", migrated.Epoch, wantEpoch)
	}
	// Bystander epoch must not change.
	by, _ := resultRT.LookupByPartition("p2")
	if by.Epoch != 1 {
		t.Errorf("bystander epoch = %d, want 1 (unchanged)", by.Epoch)
	}
}

func TestMigrator_Failover_EpochInvariant(t *testing.T) {
	p1 := makeEntry("p1", "kv", "a", "z", "dead", "dead:9000", domain.PartitionStatusActive)
	p1.Epoch = 3
	store := newMockRoutingStore(makeRT(3, []domain.RouteEntry{p1}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node2")}
	targetCtrl := &mockPSController{}
	factory := &mockPSClientFactory{byAddr: map[string]transport.PSController{
		"node2:9000": targetCtrl,
	}}

	m := NewMigrator(store, catalog, factory)
	if err := m.Failover(context.Background(), "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resultRT, _ := store.Load(context.Background())
	wantEpoch := uint64(4) // rt.Version()+1 = 3+1

	e, _ := resultRT.LookupByPartition("p1")
	if e.Epoch != wantEpoch {
		t.Errorf("failover entry epoch = %d, want %d", e.Epoch, wantEpoch)
	}
}

func TestMigrator_ResumeMigrate_EpochInvariant(t *testing.T) {
	p1 := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	p1.Epoch = 2
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{p1},
		map[string]string{"node1": "node1:9000"}))
	catalog := &mockNodeCatalog{nodes: makeNodes("node1", "node2")}

	sourceCtrl := &mockPSController{executeMigrateOutErr: provider.ErrNotFound}
	targetCtrl := &mockPSController{}
	factory := newMockPSClientFactory(nil)
	factory.byAddr["node1:9000"] = sourceCtrl
	factory.byAddr["node2:9000"] = targetCtrl

	m := NewMigrator(store, catalog, factory)
	if err := m.ResumeMigrate(context.Background(), "kv", "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resultRT, _ := store.Load(context.Background())
	wantEpoch := uint64(3) // rt.Version()+1 = 2+1

	e, _ := resultRT.LookupByPartition("p1")
	if e.Epoch != wantEpoch {
		t.Errorf("resumed entry epoch = %d, want %d", e.Epoch, wantEpoch)
	}
}
