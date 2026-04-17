package ps

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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
	return routeEntryEpoch(partitionID, actorType, start, end, nodeID, status, 0)
}

func routeEntryEpoch(partitionID, actorType, start, end, nodeID string, status domain.PartitionStatus, epoch uint64) domain.RouteEntry {
	return domain.RouteEntry{
		Partition: domain.Partition{
			ID:        partitionID,
			ActorType: actorType,
			KeyRange:  domain.KeyRange{Start: start, End: end},
		},
		NodeID:          nodeID,
		PartitionStatus: status,
		Epoch:           epoch,
	}
}

func newPartHandler(nodeID string, rt *atomic.Pointer[domain.RoutingTable], dispatchers map[string]actorDispatcher) *partitionHandler {
	return &partitionHandler{
		dispatchers: dispatchers,
		routing:     rt,
		nodeID:      nodeID,
	}
}

// newPartHandlerWithLease creates a partitionHandler with an ownership lease.
// lastHBNano is the Unix nanoseconds stored in lastHeartbeatOK (0 means not yet initialised).
func newPartHandlerWithLease(nodeID string, rt *atomic.Pointer[domain.RoutingTable], dispatchers map[string]actorDispatcher, lastHBNano int64, leaseTimeout time.Duration) *partitionHandler {
	var hb atomic.Int64
	hb.Store(lastHBNano)
	return &partitionHandler{
		dispatchers:           dispatchers,
		routing:               rt,
		nodeID:                nodeID,
		lastHeartbeatOK:       &hb,
		ownershipLeaseTimeout: leaseTimeout,
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

// ── Epoch fencing tests ───────────────────────────────────────────────────────

// TestPartitionHandler_Send_EpochZero_Skips_EpochCheck verifies that epoch=0
// (backward-compat clients) bypasses epoch validation and succeeds normally.
func TestPartitionHandler_Send_EpochZero_Skips_EpochCheck(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntryEpoch("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive, 5))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	// epoch=0 → no fencing, must succeed
	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
		Epoch:       0,
	})
	if err != nil {
		t.Errorf("epoch=0 should skip check, got err: %v", err)
	}
}

// TestPartitionHandler_Send_StaleEpoch verifies that a client with an epoch
// older than the current assignment gets FailedPrecondition (ErrPartitionMoved).
func TestPartitionHandler_Send_StaleEpoch(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	// PS has epoch=5 (current assignment); client sends epoch=3 (stale routing table).
	rt := makeRoutingPtr("node1", routeEntryEpoch("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive, 5))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
		Epoch:       3, // stale
	})
	if grpcCode(err) != codes.FailedPrecondition {
		t.Errorf("stale epoch: expected FailedPrecondition, got %v", grpcCode(err))
	}
}

// TestPartitionHandler_Send_FutureEpoch verifies that a client with an epoch
// newer than this PS's routing table gets Unavailable (ErrPartitionNotOwned).
func TestPartitionHandler_Send_FutureEpoch(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	// PS has epoch=5; client sends epoch=7 (PS routing not yet updated by PM).
	rt := makeRoutingPtr("node1", routeEntryEpoch("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive, 5))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
		Epoch:       7, // PS has stale RT
	})
	if grpcCode(err) != codes.Unavailable {
		t.Errorf("future epoch: expected Unavailable, got %v", grpcCode(err))
	}
}

// TestPartitionHandler_Send_MatchingEpoch verifies that epoch equal to the
// current assignment proceeds normally.
func TestPartitionHandler_Send_MatchingEpoch(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntryEpoch("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive, 5))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
		Epoch:       5, // matches
	})
	if err != nil {
		t.Errorf("matching epoch should succeed, got err: %v", err)
	}
}

// TestPartitionHandler_Scan_StaleEpoch mirrors the Send stale-epoch test for Scan.
func TestPartitionHandler_Scan_StaleEpoch(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntryEpoch("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive, 5))
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Scan(context.Background(), &pb.ScanRequest{
		ActorType:             "kv",
		PartitionId:           "p1",
		ExpectedKeyRangeStart: "a",
		ExpectedKeyRangeEnd:   "z",
		Epoch:                 3,
	})
	if grpcCode(err) != codes.FailedPrecondition {
		t.Errorf("stale epoch: expected FailedPrecondition, got %v", grpcCode(err))
	}
}

// ── Ownership lease tests ─────────────────────────────────────────────────────

// TestPartitionHandler_Send_LeaseExpired verifies that Send returns Unavailable
// when the ownership lease has expired (last heartbeat is older than timeout).
func TestPartitionHandler_Send_LeaseExpired(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	// Set lastHeartbeatOK to 10 seconds ago with a 3-second lease.
	expiredNS := time.Now().Add(-10 * time.Second).UnixNano()
	h := newPartHandlerWithLease("node1", rt, map[string]actorDispatcher{"kv": d}, expiredNS, 3*time.Second)

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
	})
	if grpcCode(err) != codes.Unavailable {
		t.Errorf("expired lease: expected Unavailable, got %v", grpcCode(err))
	}
}

// TestPartitionHandler_Send_LeaseValid verifies that Send succeeds when the
// last heartbeat is within the lease timeout.
func TestPartitionHandler_Send_LeaseValid(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	// Last heartbeat 1 second ago, lease timeout 3 seconds → still valid.
	recentNS := time.Now().Add(-1 * time.Second).UnixNano()
	h := newPartHandlerWithLease("node1", rt, map[string]actorDispatcher{"kv": d}, recentNS, 3*time.Second)

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
	})
	if err != nil {
		t.Errorf("valid lease: expected success, got err: %v", err)
	}
}

// TestPartitionHandler_Send_LeaseDisabled verifies that when lastHeartbeatOK is nil
// (e.g., tests that don't set up a lease), Send proceeds without lease checks.
func TestPartitionHandler_Send_LeaseDisabled(t *testing.T) {
	d := &mockDispatcher{typeID: "kv"}
	rt := makeRoutingPtr("node1", routeEntry("p1", "kv", "a", "z", "node1", domain.PartitionStatusActive))
	// newPartHandler leaves lastHeartbeatOK nil → lease check disabled.
	h := newPartHandler("node1", rt, map[string]actorDispatcher{"kv": d})

	_, err := h.Send(context.Background(), &pb.SendRequest{
		ActorType:   "kv",
		PartitionId: "p1",
		Payload:     []byte("x"),
	})
	if err != nil {
		t.Errorf("disabled lease: expected success, got err: %v", err)
	}
}
