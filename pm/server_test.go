package pm

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
)

// ─── mock pmSplitter ──────────────────────────────────────────────────────────

type mockSplitter struct {
	result string
	err    error
	calls  []splitCall
	mu     sync.Mutex
}

type splitCall struct {
	actorType, partitionID, splitKey, newPartitionID string
}

func (m *mockSplitter) Split(_ context.Context, actorType, partitionID, splitKey, newPartitionID string) (string, error) {
	m.mu.Lock()
	m.calls = append(m.calls, splitCall{actorType, partitionID, splitKey, newPartitionID})
	m.mu.Unlock()
	return m.result, m.err
}

// ─── mock pmMigrator ──────────────────────────────────────────────────────────

type mockMigrator struct {
	migrateErr       error
	failoverErr      error
	resumeMigrateErr error
	migrateCalls     []migrateCall
	resumeCalls      []migrateCall
	mu               sync.Mutex
}

type migrateCall struct {
	actorType, partitionID, targetNodeID string
}

func (m *mockMigrator) Migrate(_ context.Context, actorType, partitionID, targetNodeID string) error {
	m.mu.Lock()
	m.migrateCalls = append(m.migrateCalls, migrateCall{actorType, partitionID, targetNodeID})
	m.mu.Unlock()
	return m.migrateErr
}
func (m *mockMigrator) Failover(_ context.Context, partitionID, targetNodeID string) error {
	return m.failoverErr
}
func (m *mockMigrator) ResumeMigrate(_ context.Context, actorType, partitionID, targetNodeID string) error {
	m.mu.Lock()
	m.resumeCalls = append(m.resumeCalls, migrateCall{actorType, partitionID, targetNodeID})
	m.mu.Unlock()
	return m.resumeMigrateErr
}

// ─── mock pmMerger ────────────────────────────────────────────────────────────

type mockMerger struct {
	mergeErr       error
	resumeMergeErr error
	mergeCalled    bool
	resumeCalled   bool
}

func (m *mockMerger) Merge(_ context.Context, _, _, _ string) error {
	m.mergeCalled = true
	return m.mergeErr
}
func (m *mockMerger) ResumeMerge(_ context.Context, _, _, _ string) error {
	m.resumeCalled = true
	return m.resumeMergeErr
}

// ─── mock WorkJournal ─────────────────────────────────────────────────────────

type mockWorkJournal struct {
	mu      sync.Mutex
	entries map[string]cluster.PendingWork
	beginErr   error
	completeErr error
}

func newMockWorkJournal() *mockWorkJournal {
	return &mockWorkJournal{entries: make(map[string]cluster.PendingWork)}
}

func (j *mockWorkJournal) Begin(_ context.Context, work cluster.PendingWork) error {
	if j.beginErr != nil {
		return j.beginErr
	}
	j.mu.Lock()
	j.entries[work.ID] = work
	j.mu.Unlock()
	return nil
}

func (j *mockWorkJournal) Complete(_ context.Context, workID string) error {
	if j.completeErr != nil {
		return j.completeErr
	}
	j.mu.Lock()
	delete(j.entries, workID)
	j.mu.Unlock()
	return nil
}

func (j *mockWorkJournal) ListPending(_ context.Context) ([]cluster.PendingWork, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]cluster.PendingWork, 0, len(j.entries))
	for _, w := range j.entries {
		result = append(result, w)
	}
	return result, nil
}

func (j *mockWorkJournal) count() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.entries)
}

// ─── mock RoutingTableStore ───────────────────────────────────────────────────

type mockRTStore struct {
	mu  sync.Mutex
	rt  *domain.RoutingTable
	err error
}

func (m *mockRTStore) Save(_ context.Context, rt *domain.RoutingTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rt = rt
	return m.err
}
func (m *mockRTStore) Load(_ context.Context) (*domain.RoutingTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rt, m.err
}
func (m *mockRTStore) Watch(_ context.Context) <-chan *domain.RoutingTable {
	ch := make(chan *domain.RoutingTable)
	close(ch)
	return ch
}

// ─── helper: minimal Server ───────────────────────────────────────────────────

func newTestServer(splitter pmSplitter, migrator pmMigrator, merger pmMerger, journal cluster.WorkJournal, rtStore cluster.RoutingTableStore) *Server {
	return &Server{
		splitter:     splitter,
		migrator:     migrator,
		merger:       merger,
		workJournal:  journal,
		routingStore: rtStore,
		subscribers:  make(map[string]*subscriber),
	}
}

func makeEntry(partitionID, actorType, start, end, nodeID string, status domain.PartitionStatus) domain.RouteEntry {
	return domain.RouteEntry{
		Partition: domain.Partition{
			ID:        partitionID,
			ActorType: actorType,
			KeyRange:  domain.KeyRange{Start: start, End: end},
		},
		Node:            domain.NodeInfo{ID: nodeID, Address: nodeID + ":9000", Status: domain.NodeStatusActive},
		PartitionStatus: status,
	}
}

func makeRT(version int64, entries []domain.RouteEntry) *domain.RoutingTable {
	rt, err := domain.NewRoutingTable(version, entries)
	if err != nil {
		panic(err)
	}
	return rt
}

// ─── doSplit tests ────────────────────────────────────────────────────────────

func TestDoSplit_JournalClearedOnSuccess(t *testing.T) {
	splitter := &mockSplitter{result: "new-p"}
	journal := newMockWorkJournal()
	s := newTestServer(splitter, &mockMigrator{}, &mockMerger{}, journal, &mockRTStore{})

	_, err := s.doSplit(context.Background(), "kv", "p1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if journal.count() != 0 {
		t.Errorf("journal should be empty after success, got %d entries", journal.count())
	}
}

func TestDoSplit_JournalClearedOnFailure(t *testing.T) {
	splitter := &mockSplitter{err: errors.New("split failed")}
	journal := newMockWorkJournal()
	s := newTestServer(splitter, &mockMigrator{}, &mockMerger{}, journal, &mockRTStore{})

	_, err := s.doSplit(context.Background(), "kv", "p1", "")
	if err == nil {
		t.Fatal("expected error")
	}
	// Journal must be cleared even on failure — only PM crash leaves it.
	if journal.count() != 0 {
		t.Errorf("journal should be empty after failure, got %d entries", journal.count())
	}
}

func TestDoSplit_UsesPresetNewPartitionID(t *testing.T) {
	splitter := &mockSplitter{result: "new-p"}
	journal := newMockWorkJournal()
	s := newTestServer(splitter, &mockMigrator{}, &mockMerger{}, journal, &mockRTStore{})

	_, _ = s.doSplit(context.Background(), "kv", "p1", "")

	if len(splitter.calls) != 1 {
		t.Fatalf("expected 1 split call, got %d", len(splitter.calls))
	}
	// Pre-generated ID must be non-empty and match what was stored in journal.
	if splitter.calls[0].newPartitionID == "" {
		t.Error("newPartitionID passed to splitter should be non-empty")
	}
}

// ─── doMigrate tests ──────────────────────────────────────────────────────────

func TestDoMigrate_JournalClearedOnSuccess(t *testing.T) {
	migrator := &mockMigrator{}
	journal := newMockWorkJournal()
	s := newTestServer(&mockSplitter{}, migrator, &mockMerger{}, journal, &mockRTStore{})

	if err := s.doMigrate(context.Background(), "kv", "p1", "node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if journal.count() != 0 {
		t.Errorf("journal should be empty after success, got %d entries", journal.count())
	}
}

func TestDoMigrate_JournalClearedOnFailure(t *testing.T) {
	migrator := &mockMigrator{migrateErr: errors.New("migrate error")}
	journal := newMockWorkJournal()
	s := newTestServer(&mockSplitter{}, migrator, &mockMerger{}, journal, &mockRTStore{})

	if err := s.doMigrate(context.Background(), "kv", "p1", "node2"); err == nil {
		t.Fatal("expected error")
	}
	if journal.count() != 0 {
		t.Errorf("journal should be empty after failure, got %d entries", journal.count())
	}
}

// ─── doMerge tests ────────────────────────────────────────────────────────────

func TestDoMerge_JournalClearedOnSuccess(t *testing.T) {
	journal := newMockWorkJournal()
	s := newTestServer(&mockSplitter{}, &mockMigrator{}, &mockMerger{}, journal, &mockRTStore{})

	if err := s.doMerge(context.Background(), "kv", "lower", "upper"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if journal.count() != 0 {
		t.Errorf("journal should be empty after success, got %d entries", journal.count())
	}
}

func TestDoMerge_JournalClearedOnFailure(t *testing.T) {
	merger := &mockMerger{mergeErr: errors.New("merge error")}
	journal := newMockWorkJournal()
	s := newTestServer(&mockSplitter{}, &mockMigrator{}, merger, journal, &mockRTStore{})

	if err := s.doMerge(context.Background(), "kv", "lower", "upper"); err == nil {
		t.Fatal("expected error")
	}
	if journal.count() != 0 {
		t.Errorf("journal should be empty after failure, got %d entries", journal.count())
	}
}

// ─── resumePendingWork: Split ─────────────────────────────────────────────────

func TestResumePending_Split_NewPartitionIDAlreadyInRouting(t *testing.T) {
	newID := "new-partition-uuid"
	entry := makeEntry(newID, "kv", "m", "z", "node1", domain.PartitionStatusActive)
	rt := makeRT(2, []domain.RouteEntry{entry})
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.SplitParams{
		ActorType: "kv", PartitionID: "p1", SplitKey: "m", NewPartitionID: newID,
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeSplit,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	splitter := &mockSplitter{}
	s := newTestServer(splitter, &mockMigrator{}, &mockMerger{}, journal, rtStore)
	s.resumePendingWork(context.Background())

	// Split should not be retried — already done.
	if len(splitter.calls) != 0 {
		t.Errorf("splitter should not be called, got %d calls", len(splitter.calls))
	}
	if journal.count() != 0 {
		t.Errorf("journal should be cleared, got %d entries", journal.count())
	}
}

func TestResumePending_Split_RetriesWithSameNewPartitionID(t *testing.T) {
	presetID := "preset-partition-id"
	// Original partition still exists in routing → not yet split.
	entry := makeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive)
	rt := makeRT(1, []domain.RouteEntry{entry})
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.SplitParams{
		ActorType: "kv", PartitionID: "p1", SplitKey: "m", NewPartitionID: presetID,
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeSplit,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	splitter := &mockSplitter{result: presetID}
	s := newTestServer(splitter, &mockMigrator{}, &mockMerger{}, journal, rtStore)
	s.resumePendingWork(context.Background())

	if len(splitter.calls) != 1 {
		t.Fatalf("expected 1 split call, got %d", len(splitter.calls))
	}
	if splitter.calls[0].newPartitionID != presetID {
		t.Errorf("resume should use same newPartitionID, got %q want %q", splitter.calls[0].newPartitionID, presetID)
	}
	if journal.count() != 0 {
		t.Errorf("journal should be cleared after successful resume, got %d entries", journal.count())
	}
}

// ─── resumePendingWork: Migrate — Draining ────────────────────────────────────

func TestResumePending_Migrate_DrainingUsesResumeMigrate(t *testing.T) {
	// Partition is stuck in Draining — PM crashed mid-migrate.
	entry := makeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusDraining)
	rt := makeRT(2, []domain.RouteEntry{entry})
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.MigrateParams{
		ActorType: "kv", PartitionID: "p1", TargetNodeID: "node2",
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeMigrate,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	migrator := &mockMigrator{}
	s := newTestServer(&mockSplitter{}, migrator, &mockMerger{}, journal, rtStore)
	s.resumePendingWork(context.Background())

	if len(migrator.resumeCalls) != 1 {
		t.Fatalf("expected ResumeMigrate to be called, got %d calls", len(migrator.resumeCalls))
	}
	if len(migrator.migrateCalls) != 0 {
		t.Error("Migrate should not be called when partition is Draining")
	}
	if journal.count() != 0 {
		t.Errorf("journal should be cleared, got %d entries", journal.count())
	}
}

func TestResumePending_Migrate_ActiveUsesNormalMigrate(t *testing.T) {
	// Partition is Active (PM crashed before even setting Draining).
	entry := makeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive)
	rt := makeRT(1, []domain.RouteEntry{entry})
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.MigrateParams{
		ActorType: "kv", PartitionID: "p1", TargetNodeID: "node2",
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeMigrate,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	migrator := &mockMigrator{}
	s := newTestServer(&mockSplitter{}, migrator, &mockMerger{}, journal, rtStore)
	s.resumePendingWork(context.Background())

	if len(migrator.migrateCalls) != 1 {
		t.Fatalf("expected Migrate to be called, got %d calls", len(migrator.migrateCalls))
	}
	if len(migrator.resumeCalls) != 0 {
		t.Error("ResumeMigrate should not be called for Active partition")
	}
}

func TestResumePending_Migrate_AlreadyOnTarget_ClearsJournal(t *testing.T) {
	// Partition is already on the target node → migrate completed before crash.
	entry := makeEntry("p1", "kv", "a", "z", "node2", domain.PartitionStatusActive)
	rt := makeRT(3, []domain.RouteEntry{entry})
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.MigrateParams{
		ActorType: "kv", PartitionID: "p1", TargetNodeID: "node2",
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeMigrate,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	migrator := &mockMigrator{}
	s := newTestServer(&mockSplitter{}, migrator, &mockMerger{}, journal, rtStore)
	s.resumePendingWork(context.Background())

	if len(migrator.migrateCalls) != 0 || len(migrator.resumeCalls) != 0 {
		t.Error("no migrate calls expected when already on target")
	}
	if journal.count() != 0 {
		t.Errorf("journal should be cleared, got %d entries", journal.count())
	}
}

// ─── resumePendingWork: Merge — Draining ─────────────────────────────────────

func TestResumePending_Merge_DrainingUsesResumeMerge(t *testing.T) {
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "m", "node1", domain.PartitionStatusDraining),
		makeEntry("upper", "kv", "m", "z", "node1", domain.PartitionStatusDraining),
	}
	rt := makeRT(2, entries)
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.MergeParams{
		ActorType: "kv", LowerID: "lower", UpperID: "upper",
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeMerge,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	merger := &mockMerger{}
	s := newTestServer(&mockSplitter{}, &mockMigrator{}, merger, journal, rtStore)
	s.resumePendingWork(context.Background())

	if !merger.resumeCalled {
		t.Error("ResumeMerge should be called when partitions are Draining")
	}
	if merger.mergeCalled {
		t.Error("Merge should not be called directly for Draining partitions")
	}
	if journal.count() != 0 {
		t.Errorf("journal should be cleared, got %d entries", journal.count())
	}
}

func TestResumePending_Merge_UpperGone_ClearsJournal(t *testing.T) {
	// Upper partition already gone from routing → merge completed.
	entries := []domain.RouteEntry{
		makeEntry("lower", "kv", "a", "z", "node1", domain.PartitionStatusActive),
	}
	rt := makeRT(3, entries)
	rtStore := &mockRTStore{rt: rt}

	params, _ := json.Marshal(cluster.MergeParams{
		ActorType: "kv", LowerID: "lower", UpperID: "upper",
	})
	work := cluster.PendingWork{
		ID: "w1", Type: cluster.WorkTypeMerge,
		Params: params, StartedAt: time.Now(),
	}
	journal := newMockWorkJournal()
	_ = journal.Begin(context.Background(), work)

	merger := &mockMerger{}
	s := newTestServer(&mockSplitter{}, &mockMigrator{}, merger, journal, rtStore)
	s.resumePendingWork(context.Background())

	if merger.mergeCalled || merger.resumeCalled {
		t.Error("no merge calls expected when upper is gone")
	}
	if journal.count() != 0 {
		t.Errorf("journal should be cleared, got %d entries", journal.count())
	}
}
