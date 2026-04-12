package ps

import (
	"context"
	"log/slog"
	"sync"
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

	dispatchers     map[string]actorDispatcher
	nodeID          string
	routing         *atomic.Pointer[domain.RoutingTable]
	fenced          *atomic.Bool // set to true when node is isolated; rejects control-plane commands
	partitionEpochs sync.Map     // partitionID → uint64: epoch assigned by PM at PreparePartition
}

// isFenced returns true if this node has been isolated and is shutting down.
func (h *controlHandler) isFenced() bool {
	return h.fenced != nil && h.fenced.Load()
}

// ExecuteSplit handles a split command from the PM.
// If req.SplitKey is empty, the split key is determined via the Actor's SplitHint() or the key range midpoint.
// The actually used split key is included in the response.
func (h *controlHandler) ExecuteSplit(
	ctx context.Context,
	req *pb.ExecuteSplitRequest,
) (*pb.ExecuteSplitResponse, error) {
	if h.isFenced() {
		return nil, status.Error(codes.Unavailable, "node is fenced")
	}
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
	if h.isFenced() {
		return nil, status.Error(codes.Unavailable, "node is fenced")
	}
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
	if h.isFenced() {
		return nil, status.Error(codes.Unavailable, "node is fenced")
	}
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

// Ping responds to a liveness check from the PM.
// Used by PM after lease expiry to distinguish real failures from etcd overload false positives.
func (h *controlHandler) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{}, nil
}

// PreparePartition handles a partition-load command from the PM.
// The epoch in the request must be >= the stored epoch for this partition.
// This protects against stale PM directives after a PM leader change.
func (h *controlHandler) PreparePartition(
	ctx context.Context,
	req *pb.PreparePartitionRequest,
) (*pb.PreparePartitionResponse, error) {
	if h.isFenced() {
		return nil, status.Error(codes.Unavailable, "node is fenced")
	}
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}

	// Validate epoch: reject commands from a stale PM (lower epoch than already stored).
	if req.Epoch > 0 {
		if stored, exists := h.partitionEpochs.Load(req.PartitionId); exists {
			if req.Epoch < stored.(uint64) {
				slog.Warn("ctrl: PreparePartition rejected: stale epoch",
					"node", h.nodeID, "partition", req.PartitionId,
					"request_epoch", req.Epoch, "stored_epoch", stored.(uint64))
				return nil, status.Errorf(codes.FailedPrecondition,
					"stale epoch %d for partition %s (current: %d)",
					req.Epoch, req.PartitionId, stored.(uint64))
			}
		}
	}

	slog.Info("ctrl: PreparePartition", "node", h.nodeID, "actor_type", req.ActorType,
		"partition", req.PartitionId, "epoch", req.Epoch)
	if err := d.Activate(ctx, req.PartitionId); err != nil {
		slog.Error("ctrl: PreparePartition failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}

	// Store the epoch after successful activation.
	if req.Epoch > 0 {
		h.partitionEpochs.Store(req.PartitionId, req.Epoch)
	}

	slog.Info("ctrl: PreparePartition done", "node", h.nodeID, "partition", req.PartitionId, "epoch", req.Epoch)
	return &pb.PreparePartitionResponse{}, nil
}
