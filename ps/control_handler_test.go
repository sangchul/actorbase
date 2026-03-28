package ps

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/engine"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
)

// ── mock actorDispatcher ──────────────────────────────────────────────────────

type mockDispatcher struct {
	typeID      string
	splitResult string
	splitErr    error
	evictErr    error
	mergeErr    error
	activateErr error
	sendErr     error
	stats       []engine.PartitionStats
}

func (m *mockDispatcher) Send(_ context.Context, _ string, payload []byte) ([]byte, error) {
	return payload, m.sendErr
}
func (m *mockDispatcher) Evict(_ context.Context, _ string) error    { return m.evictErr }
func (m *mockDispatcher) EvictAll(_ context.Context) error           { return nil }
func (m *mockDispatcher) Activate(_ context.Context, _ string) error { return m.activateErr }
func (m *mockDispatcher) Split(_ context.Context, _, _, _, _, _ string) (string, error) {
	return m.splitResult, m.splitErr
}
func (m *mockDispatcher) Merge(_ context.Context, _, _ string) error { return m.mergeErr }
func (m *mockDispatcher) StartSchedulers(_ context.Context, _, _, _ time.Duration) {}
func (m *mockDispatcher) GetStats() []engine.PartitionStats           { return m.stats }
func (m *mockDispatcher) TypeID() string                              { return m.typeID }

// ── helper ────────────────────────────────────────────────────────────────────

func newCtrlHandler(dispatchers map[string]actorDispatcher) *controlHandler {
	var rt atomic.Pointer[domain.RoutingTable]
	return &controlHandler{
		dispatchers: dispatchers,
		nodeID:      "node1",
		routing:     &rt,
	}
}

func grpcCode(err error) codes.Code {
	return status.Code(err)
}

// ── ExecuteSplit ──────────────────────────────────────────────────────────────

func TestControlHandler_ExecuteSplit_Success(t *testing.T) {
	d := &mockDispatcher{typeID: "kv", splitResult: "m"}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	resp, err := h.ExecuteSplit(context.Background(), &pb.ExecuteSplitRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		SplitKey:    "m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.SplitKey != "m" {
		t.Errorf("SplitKey = %q, want %q", resp.SplitKey, "m")
	}
}

func TestControlHandler_ExecuteSplit_UnknownActorType(t *testing.T) {
	h := newCtrlHandler(map[string]actorDispatcher{})

	_, err := h.ExecuteSplit(context.Background(), &pb.ExecuteSplitRequest{
		ActorType: "unknown",
	})
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}

func TestControlHandler_ExecuteSplit_DispatcherError(t *testing.T) {
	d := &mockDispatcher{typeID: "kv", splitErr: errors.New("split failed")}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.ExecuteSplit(context.Background(), &pb.ExecuteSplitRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── ExecuteMigrateOut ─────────────────────────────────────────────────────────

func TestControlHandler_ExecuteMigrateOut_Success(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.ExecuteMigrateOut(context.Background(), &pb.ExecuteMigrateOutRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestControlHandler_ExecuteMigrateOut_UnknownActorType(t *testing.T) {
	h := newCtrlHandler(map[string]actorDispatcher{})

	_, err := h.ExecuteMigrateOut(context.Background(), &pb.ExecuteMigrateOutRequest{
		ActorType: "unknown",
	})
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}

func TestControlHandler_ExecuteMigrateOut_EvictError(t *testing.T) {
	d := &mockDispatcher{typeID: "kv", evictErr: errors.New("evict failed")}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.ExecuteMigrateOut(context.Background(), &pb.ExecuteMigrateOutRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── ExecuteMerge ──────────────────────────────────────────────────────────────

func TestControlHandler_ExecuteMerge_Success(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.ExecuteMerge(context.Background(), &pb.ExecuteMergeRequest{
		ActorType:        "kv",
		LowerPartitionId: "lower",
		UpperPartitionId: "upper",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestControlHandler_ExecuteMerge_UnknownActorType(t *testing.T) {
	h := newCtrlHandler(map[string]actorDispatcher{})

	_, err := h.ExecuteMerge(context.Background(), &pb.ExecuteMergeRequest{
		ActorType: "unknown",
	})
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}

func TestControlHandler_ExecuteMerge_MergeError(t *testing.T) {
	d := &mockDispatcher{typeID: "kv", mergeErr: errors.New("merge failed")}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.ExecuteMerge(context.Background(), &pb.ExecuteMergeRequest{
		ActorType:        "kv",
		LowerPartitionId: "lower",
		UpperPartitionId: "upper",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── PreparePartition ──────────────────────────────────────────────────────────

func TestControlHandler_PreparePartition_Success(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.PreparePartition(context.Background(), &pb.PreparePartitionRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestControlHandler_PreparePartition_UnknownActorType(t *testing.T) {
	h := newCtrlHandler(map[string]actorDispatcher{})

	_, err := h.PreparePartition(context.Background(), &pb.PreparePartitionRequest{
		ActorType: "unknown",
	})
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}

func TestControlHandler_PreparePartition_ActivateError(t *testing.T) {
	d := &mockDispatcher{typeID: "kv", activateErr: errors.New("activate failed")}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d})

	_, err := h.PreparePartition(context.Background(), &pb.PreparePartitionRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── GetStats ──────────────────────────────────────────────────────────────────

func TestControlHandler_GetStats_AggregatesAllDispatchers(t *testing.T) {
	d1 := &mockDispatcher{
		typeID: "kv",
		stats: []engine.PartitionStats{
			{PartitionID: "p1", RPS: 10},
			{PartitionID: "p2", RPS: 20},
		},
	}
	d2 := &mockDispatcher{
		typeID: "object",
		stats: []engine.PartitionStats{
			{PartitionID: "p3", RPS: 5},
		},
	}
	h := newCtrlHandler(map[string]actorDispatcher{"kv": d1, "object": d2})

	resp, err := h.GetStats(context.Background(), &pb.GetStatsRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if int(resp.PartitionCount) != 3 {
		t.Errorf("PartitionCount = %d, want 3", resp.PartitionCount)
	}
	// nodeRPS = 10 + 20 + 5 = 35
	if resp.NodeRps != 35 {
		t.Errorf("NodeRps = %v, want 35", resp.NodeRps)
	}
}

func TestControlHandler_GetStats_EmptyDispatchers(t *testing.T) {
	h := newCtrlHandler(map[string]actorDispatcher{})
	resp, err := h.GetStats(context.Background(), &pb.GetStatsRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.PartitionCount != 0 {
		t.Errorf("expected 0 partitions, got %d", resp.PartitionCount)
	}
}
