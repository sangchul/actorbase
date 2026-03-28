package ps

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/sangchul/actorbase/internal/domain"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/provider"
)

// ── helper ────────────────────────────────────────────────────────────────────

func makeRoutingPtr(nodeID string, entries ...domain.RouteEntry) *atomic.Pointer[domain.RoutingTable] {
	rt, err := domain.NewRoutingTable(1, entries)
	if err != nil {
		panic(err)
	}
	var p atomic.Pointer[domain.RoutingTable]
	p.Store(rt)
	return &p
}

func routeEntry(partitionID, actorType, start, end, nodeID string, status domain.PartitionStatus) domain.RouteEntry {
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

func newPartHandler(nodeID string, rt *atomic.Pointer[domain.RoutingTable], dispatchers map[string]actorDispatcher) *partitionHandler {
	return &partitionHandler{
		dispatchers: dispatchers,
		routing:     rt,
		nodeID:      nodeID,
	}
}

// ── Send tests ────────────────────────────────────────────────────────────────

func TestPartitionHandler_Send_Success(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	resp, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("hello"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(resp.Payload) != "hello" {
		t.Errorf("payload = %q, want hello", resp.Payload)
	}
}

func TestPartitionHandler_Send_UnknownActorType(t *testing.T) {
	rt := makeRoutingPtr("node1")
	h := newPartHandler("node1", rt, map[string]actorDispatcher{})

	_, err := h.Send(context.Background(), &pb.SendRequest{ActorType: "unknown"})
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Send_RoutingTableNil(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	var rt atomic.Pointer[domain.RoutingTable] // nil
	h := newPartHandler("node1", &rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if grpcCode(err) != codes.Unavailable {
		t.Errorf("expected codes.Unavailable, got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Send_PartitionNotInRouting(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1") // empty routing
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "missing",
	})
	if grpcCode(err) != codes.Unavailable {
		t.Errorf("expected codes.Unavailable, got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Send_NodeIDMismatch(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	// Partition is on node2, but handler is node1.
	rt := makeRoutingPtr("node2", routeEntry("p1", "kv", "a", "z", "node2", domain.PartitionStatusActive))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if grpcCode(err) != codes.Unavailable {
		t.Errorf("expected codes.Unavailable (node mismatch), got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Send_PartitionDraining(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusDraining))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if grpcCode(err) != codes.ResourceExhausted {
		t.Errorf("expected codes.ResourceExhausted (ErrPartitionBusy), got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Send_DispatcherError(t *testing.T) {
	d := &mockDispatcher{typeID: "kv", sendErr: provider.ErrNotFound}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error from dispatcher")
	}
	// ErrNotFound maps to codes.NotFound via ToGRPCStatus.
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}

// ── Scan tests ────────────────────────────────────────────────────────────────

func TestPartitionHandler_Scan_Success(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	resp, err := h.Scan(context.Background(), &pb.ScanRequest{
		ActorType:             "kv",
		PartitionId:           "p1",
		Payload:               []byte("scan"),
		ExpectedKeyRangeStart: "a",
		ExpectedKeyRangeEnd:   "z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(resp.Payload) != "scan" {
		t.Errorf("payload = %q, want scan", resp.Payload)
	}
}

func TestPartitionHandler_Scan_KeyRangeMismatch(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	// Actual range is "a"-"z", but SDK expects "a"-"m" (stale routing after split).
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Scan(context.Background(), &pb.ScanRequest{
		ActorType:             "kv",
		PartitionId:           "p1",
		ExpectedKeyRangeStart: "a",
		ExpectedKeyRangeEnd:   "m", // mismatch
	})
	if grpcCode(err) != codes.FailedPrecondition {
		t.Errorf("expected codes.FailedPrecondition (ErrPartitionMoved), got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Scan_DrainingReturnsResourceExhausted(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusDraining))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Scan(context.Background(), &pb.ScanRequest{
		ActorType:   "kv",
		PartitionId: "p1",
	})
	if grpcCode(err) != codes.ResourceExhausted {
		t.Errorf("expected codes.ResourceExhausted, got %v", grpcCode(err))
	}
}

func TestPartitionHandler_Scan_UnknownActorType(t *testing.T) {
	rt := makeRoutingPtr("node1")
	h := newPartHandler("node1", rt, map[string]actorDispatcher{})

	_, err := h.Scan(context.Background(), &pb.ScanRequest{ActorType: "unknown"})
	if grpcCode(err) != codes.NotFound {
		t.Errorf("expected codes.NotFound, got %v", grpcCode(err))
	}
}
