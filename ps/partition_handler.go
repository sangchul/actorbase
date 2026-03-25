package ps

import (
	"context"
	"sync/atomic"

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

	dispatchers map[string]actorDispatcher
	routing     *atomic.Pointer[domain.RoutingTable]
	nodeID      string
}

// Send forwards a request to the Actor for req.ActorType and returns the response.
func (h *partitionHandler) Send(
	ctx context.Context,
	req *pb.SendRequest,
) (*pb.SendResponse, error) {
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
	if entry.Node.ID != h.nodeID {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 4. Reject if the partition is in Draining status.
	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return nil, status.Error(codes.ResourceExhausted, provider.ErrPartitionBusy.Error())
	}

	// 5. Forward to the Actor via the dispatcher (includes deserialization).
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
	if entry.Node.ID != h.nodeID {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
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
