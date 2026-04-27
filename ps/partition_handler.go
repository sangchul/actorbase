package ps

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/provider"
)

// partitionHandler is the PartitionService gRPC handler.
// Handles SDK → PS data-plane requests.
// Selects the correct actorDispatcher based on req.ActorType.
type partitionHandler struct {
	pb.UnimplementedPartitionServiceServer

	dispatchers           map[string]actorDispatcher
	routing               *atomic.Pointer[domain.RoutingTable]
	nodeID                string
	fenced                *atomic.Bool  // set to true when node is isolated; rejects all data-plane requests
	lastHeartbeatOK       *atomic.Int64 // Unix nanoseconds of the last successful PM heartbeat
	ownershipLeaseTimeout time.Duration // max time without a heartbeat before rejecting data-plane requests
}

// isLeaseExpired returns true when the ownership lease has expired:
// the last successful PM heartbeat is older than ownershipLeaseTimeout.
// Returns false when the lease is disabled (lastHeartbeatOK == nil or timeout == 0).
func (h *partitionHandler) isLeaseExpired() bool {
	if h.lastHeartbeatOK == nil || h.ownershipLeaseTimeout <= 0 {
		return false
	}
	lastNS := h.lastHeartbeatOK.Load()
	if lastNS == 0 {
		return false // not yet initialised (server hasn't started heartbeat loop)
	}
	return time.Since(time.Unix(0, lastNS)) > h.ownershipLeaseTimeout
}

// Send forwards a request to the Actor for req.ActorType and returns the response.
func (h *partitionHandler) Send(
	ctx context.Context,
	req *pb.SendRequest,
) (*pb.SendResponse, error) {
	if h.fenced != nil && h.fenced.Load() {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}
	if h.isLeaseExpired() {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 1. Look up the dispatcher for the actor type.
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}

	// 2. Load the routing table.
	rt := h.routing.Load()
	if rt == nil {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 3. Verify the partition exists and is owned by this node.
	entry, ok := rt.LookupByPartition(req.PartitionId)
	if !ok {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}
	if entry.NodeID != h.nodeID {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 4. Epoch fencing: reject writes from stale or future routing tables.
	//    req.Epoch == 0 means the client does not send an epoch (backward-compat).
	if req.Epoch != 0 {
		if req.Epoch < entry.Epoch {
			// Client has a stale routing table (epoch predates the current assignment).
			// The client should refresh the routing table and retry.
			return nil, status.Error(codes.FailedPrecondition, provider.ErrPartitionMoved.Error())
		}
		if req.Epoch > entry.Epoch {
			// This PS itself has a stale routing table (the PM re-assigned the partition
			// to a new epoch, but this PS has not yet received the update).
			// The client should retry; this PS will catch up shortly.
			return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
		}
	}

	// 5. Reject if the partition is in Draining status.
	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return nil, status.Error(codes.ResourceExhausted, provider.ErrPartitionBusy.Error())
	}

	// 6. Forward to the Actor via the dispatcher (includes deserialization).
	if h.isLeaseExpired() {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}
	payload, err := d.Send(ctx, req.PartitionId, req.Payload)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}

	return &pb.SendResponse{Payload: payload}, nil
}

// Scan forwards an SDK range-query request to the Actor.
// Behaves like Send but also validates the expected key range to detect stale routing.
func (h *partitionHandler) Scan(
	ctx context.Context,
	req *pb.ScanRequest,
) (*pb.ScanResponse, error) {
	if h.fenced != nil && h.fenced.Load() {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}
	if h.isLeaseExpired() {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}

	rt := h.routing.Load()
	if rt == nil {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	entry, ok := rt.LookupByPartition(req.PartitionId)
	if !ok {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}
	if entry.NodeID != h.nodeID {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// Epoch fencing (same semantics as Send).
	if req.Epoch != 0 {
		if req.Epoch < entry.Epoch {
			return nil, status.Error(codes.FailedPrecondition, provider.ErrPartitionMoved.Error())
		}
		if req.Epoch > entry.Epoch {
			return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
		}
	}

	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return nil, status.Error(codes.ResourceExhausted, provider.ErrPartitionBusy.Error())
	}

	// If the partition key range differs from what the SDK expects, the routing table is stale (the partition was split).
	if req.ExpectedKeyRangeStart != entry.Partition.KeyRange.Start ||
		req.ExpectedKeyRangeEnd != entry.Partition.KeyRange.End {
		return nil, status.Error(codes.FailedPrecondition, provider.ErrPartitionMoved.Error())
	}

	payload, err := d.Send(ctx, req.PartitionId, req.Payload)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}

	return &pb.ScanResponse{Payload: payload}, nil
}
