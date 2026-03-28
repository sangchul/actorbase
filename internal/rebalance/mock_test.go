package rebalance

import (
	"context"
	"sync"

	"github.com/sangchul/actorbase/internal/cluster"
	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
)

// ─── mock RoutingTableStore ───────────────────────────────────────────────────

type mockRoutingStore struct {
	mu  sync.Mutex
	rt  *domain.RoutingTable
	err error
}

func newMockRoutingStore(rt *domain.RoutingTable) *mockRoutingStore {
	return &mockRoutingStore{rt: rt}
}

func (m *mockRoutingStore) Save(_ context.Context, rt *domain.RoutingTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.rt = rt
	return nil
}

func (m *mockRoutingStore) Load(_ context.Context) (*domain.RoutingTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rt, m.err
}

func (m *mockRoutingStore) Watch(_ context.Context) <-chan *domain.RoutingTable {
	ch := make(chan *domain.RoutingTable)
	close(ch)
	return ch
}

// ─── mock NodeCatalog ─────────────────────────────────────────────────────────

type mockNodeCatalog struct {
	nodes []domain.NodeInfo
	err   error
}

func (m *mockNodeCatalog) ListNodes(_ context.Context) ([]domain.NodeInfo, error) {
	return m.nodes, m.err
}
func (m *mockNodeCatalog) GetNode(_ context.Context, nodeID string) (domain.NodeInfo, bool, error) {
	for _, n := range m.nodes {
		if n.ID == nodeID {
			return n, true, m.err
		}
	}
	return domain.NodeInfo{}, false, m.err
}
func (m *mockNodeCatalog) AddNode(_ context.Context, _ domain.NodeInfo) error { return nil }
func (m *mockNodeCatalog) UpdateStatus(_ context.Context, _ string, _ domain.NodeStatus) error {
	return nil
}
func (m *mockNodeCatalog) RemoveNode(_ context.Context, _ string) error { return nil }

// ─── mock PSController ────────────────────────────────────────────────────────

type mockPSController struct {
	// ExecuteSplit
	executeSplitKey string
	executeSplitErr error

	// ExecuteMigrateOut
	executeMigrateOutErr error
	migrateOutCalled     bool

	// PreparePartition
	preparePartitionErr error
	preparePartitionCalled bool

	// ExecuteMerge
	executeMergeErr error
	mergeCalled     bool

	// GetStats
	statsResp *pb.GetStatsResponse
	statsErr  error
}

func (m *mockPSController) ExecuteSplit(_ context.Context, _, _, splitKey, _, _, _ string) (string, error) {
	if m.executeSplitKey != "" {
		return m.executeSplitKey, m.executeSplitErr
	}
	return splitKey, m.executeSplitErr
}

func (m *mockPSController) ExecuteMigrateOut(_ context.Context, _, _, _, _ string) error {
	m.migrateOutCalled = true
	return m.executeMigrateOutErr
}

func (m *mockPSController) PreparePartition(_ context.Context, _, _, _, _ string) error {
	m.preparePartitionCalled = true
	return m.preparePartitionErr
}

func (m *mockPSController) ExecuteMerge(_ context.Context, _, _, _ string) error {
	m.mergeCalled = true
	return m.executeMergeErr
}

func (m *mockPSController) GetStats(_ context.Context) (*pb.GetStatsResponse, error) {
	return m.statsResp, m.statsErr
}

// ─── mock PSClientFactory ─────────────────────────────────────────────────────

type mockPSClientFactory struct {
	ctrl transport.PSController
	err  error
	// per-address overrides
	byAddr map[string]transport.PSController
}

func newMockPSClientFactory(ctrl transport.PSController) *mockPSClientFactory {
	return &mockPSClientFactory{ctrl: ctrl, byAddr: make(map[string]transport.PSController)}
}

func (m *mockPSClientFactory) GetClient(addr string) (transport.PSController, error) {
	if m.err != nil {
		return nil, m.err
	}
	if c, ok := m.byAddr[addr]; ok {
		return c, nil
	}
	return m.ctrl, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func makeRT(version int64, entries []domain.RouteEntry) *domain.RoutingTable {
	rt, err := domain.NewRoutingTable(version, entries)
	if err != nil {
		panic(err)
	}
	return rt
}

func makeEntry(partitionID, actorType, start, end, nodeID, nodeAddr string, status domain.PartitionStatus) domain.RouteEntry {
	return domain.RouteEntry{
		Partition: domain.Partition{
			ID:        partitionID,
			ActorType: actorType,
			KeyRange:  domain.KeyRange{Start: start, End: end},
		},
		Node: domain.NodeInfo{
			ID:      nodeID,
			Address: nodeAddr,
			Status:  domain.NodeStatusActive,
		},
		PartitionStatus: status,
	}
}

// Ensure mock implements the cluster.NodeCatalog interface at compile time.
var _ cluster.NodeCatalog = (*mockNodeCatalog)(nil)
var _ cluster.RoutingTableStore = (*mockRoutingStore)(nil)
var _ transport.PSController = (*mockPSController)(nil)
var _ transport.PSClientFactory = (*mockPSClientFactory)(nil)
