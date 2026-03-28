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
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
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
	if e.Node.ID != "node2" {
		t.Errorf("partition node = %q, want %q", e.Node.ID, "node2")
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
	store := newMockRoutingStore(makeRT(1, []domain.RouteEntry{entry}))
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
	if e.Node.ID != "node1" {
		t.Error("routing should remain on source node after MigrateOut failure")
	}
}

// ─── ResumeMigrate ────────────────────────────────────────────────────────────

func TestMigrator_ResumeMigrate_SourceAlreadyEvicted(t *testing.T) {
	// Routing is Draining (PM crashed after MigrateOut but before PreparePartition).
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{entry}))
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
	if e.Node.ID != "node2" {
		t.Errorf("partition node = %q, want %q", e.Node.ID, "node2")
	}
	if e.PartitionStatus != domain.PartitionStatusActive {
		t.Error("partition should be Active after ResumeMigrate")
	}
}

func TestMigrator_ResumeMigrate_SourceNotOwned(t *testing.T) {
	entry := makeEntry("p1", "kv", "a", "z", "node1", "node1:9000", domain.PartitionStatusDraining)
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{entry}))
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
	store := newMockRoutingStore(makeRT(2, []domain.RouteEntry{entry}))
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
	if e.Node.ID != "node2" {
		t.Errorf("partition node = %q, want %q", e.Node.ID, "node2")
	}
}
