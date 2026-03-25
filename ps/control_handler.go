package ps

import (
	"context"
	"log/slog"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/engine"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
)

// controlHandler is the PartitionControlService gRPC handler.
// Handles PM → PS control-plane requests.
// Selects the correct actorDispatcher based on req.ActorType.
type controlHandler struct {
	pb.UnimplementedPartitionControlServiceServer

	dispatchers map[string]actorDispatcher
	nodeID      string
	routing     *atomic.Pointer[domain.RoutingTable]
}

// ExecuteSplit handles a split command from the PM.
// If req.SplitKey is empty, the split key is determined via the Actor's SplitHint() or the key range midpoint.
// The actually used split key is included in the response.
func (h *controlHandler) ExecuteSplit(
	ctx context.Context,
	req *pb.ExecuteSplitRequest,
) (*pb.ExecuteSplitResponse, error) {
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}

	// If the request contains no key range, look it up in the routing table (ensures midpoint fallback).
	keyRangeStart := req.KeyRangeStart
	keyRangeEnd := req.KeyRangeEnd
	if keyRangeStart == "" && keyRangeEnd == "" && req.SplitKey == "" {
		if rt := h.routing.Load(); rt != nil {
			if entry, found := rt.LookupByPartition(req.PartitionId); found {
				keyRangeStart = entry.Partition.KeyRange.Start
				keyRangeEnd = entry.Partition.KeyRange.End
			}
		}
	}

	slog.Info("ctrl: ExecuteSplit", "node", h.nodeID, "actor_type", req.ActorType,
		"partition", req.PartitionId, "split_key", req.SplitKey, "new_partition", req.NewPartitionId)
	usedKey, err := d.Split(ctx, req.PartitionId, req.SplitKey, keyRangeStart, keyRangeEnd, req.NewPartitionId)
	if err != nil {
		slog.Error("ctrl: ExecuteSplit failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: ExecuteSplit done", "node", h.nodeID, "partition", req.PartitionId, "used_key", usedKey)
	return &pb.ExecuteSplitResponse{SplitKey: usedKey}, nil
}

// engineMidpoint is an alias for engine.KeyRangeMidpoint. The controlHandler does not call it
// directly; this declaration exists solely to keep the package reference alive.
var _ = engine.KeyRangeMidpoint

// ExecuteMigrateOut handles a migrate-out command from the PM.
func (h *controlHandler) ExecuteMigrateOut(
	ctx context.Context,
	req *pb.ExecuteMigrateOutRequest,
) (*pb.ExecuteMigrateOutResponse, error) {
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}
	slog.Info("ctrl: ExecuteMigrateOut", "node", h.nodeID, "actor_type", req.ActorType,
		"partition", req.PartitionId, "target", req.TargetNodeId)
	if err := d.Evict(ctx, req.PartitionId); err != nil {
		slog.Error("ctrl: ExecuteMigrateOut failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: ExecuteMigrateOut done", "node", h.nodeID, "partition", req.PartitionId)
	return &pb.ExecuteMigrateOutResponse{}, nil
}

// ExecuteMerge handles a merge command from the PM.
// The lower partition absorbs the state of the upper partition.
func (h *controlHandler) ExecuteMerge(
	ctx context.Context,
	req *pb.ExecuteMergeRequest,
) (*pb.ExecuteMergeResponse, error) {
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}
	slog.Info("ctrl: ExecuteMerge", "node", h.nodeID, "actor_type", req.ActorType,
		"lower", req.LowerPartitionId, "upper", req.UpperPartitionId)
	if err := d.Merge(ctx, req.LowerPartitionId, req.UpperPartitionId); err != nil {
		slog.Error("ctrl: ExecuteMerge failed", "node", h.nodeID,
			"lower", req.LowerPartitionId, "upper", req.UpperPartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: ExecuteMerge done", "node", h.nodeID,
		"lower", req.LowerPartitionId, "upper", req.UpperPartitionId)
	return &pb.ExecuteMergeResponse{}, nil
}

// GetStats handles a stats-query request from the PM.
func (h *controlHandler) GetStats(
	_ context.Context,
	_ *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	var allPartitions []*pb.PartitionStatsProto
	var nodeRPS float64

	for _, d := range h.dispatchers {
		typeID := d.TypeID()
		for _, s := range d.GetStats() {
			allPartitions = append(allPartitions, &pb.PartitionStatsProto{
				PartitionId: s.PartitionID,
				ActorType:   typeID,
				KeyCount:    s.KeyCount,
				Rps:         s.RPS,
			})
			nodeRPS += s.RPS
		}
	}

	return &pb.GetStatsResponse{
		Partitions:     allPartitions,
		NodeRps:        nodeRPS,
		PartitionCount: int32(len(allPartitions)),
	}, nil
}

// PreparePartition handles a partition-load command from the PM.
func (h *controlHandler) PreparePartition(
	ctx context.Context,
	req *pb.PreparePartitionRequest,
) (*pb.PreparePartitionResponse, error) {
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}
	slog.Info("ctrl: PreparePartition", "node", h.nodeID, "actor_type", req.ActorType,
		"partition", req.PartitionId)
	if err := d.Activate(ctx, req.PartitionId); err != nil {
		slog.Error("ctrl: PreparePartition failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: PreparePartition done", "node", h.nodeID, "partition", req.PartitionId)
	return &pb.PreparePartitionResponse{}, nil
}
