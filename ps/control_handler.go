package ps

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oomymy/actorbase/internal/transport"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
)

// controlHandler는 PartitionControlService gRPC 핸들러.
// PM → PS control plane 요청을 처리한다.
// req.ActorType으로 올바른 actorDispatcher를 선택한다.
type controlHandler struct {
	pb.UnimplementedPartitionControlServiceServer

	dispatchers map[string]actorDispatcher
	nodeID      string
}

// ExecuteSplit은 PM의 split 명령을 처리한다.
func (h *controlHandler) ExecuteSplit(
	ctx context.Context,
	req *pb.ExecuteSplitRequest,
) (*pb.ExecuteSplitResponse, error) {
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}
	slog.Info("ctrl: ExecuteSplit", "node", h.nodeID, "actor_type", req.ActorType,
		"partition", req.PartitionId, "split_key", req.SplitKey, "new_partition", req.NewPartitionId)
	if err := d.Split(ctx, req.PartitionId, req.SplitKey, req.NewPartitionId); err != nil {
		slog.Error("ctrl: ExecuteSplit failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: ExecuteSplit done", "node", h.nodeID, "partition", req.PartitionId)
	return &pb.ExecuteSplitResponse{}, nil
}

// ExecuteMigrateOut은 PM의 migration out 명령을 처리한다.
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

// PreparePartition은 PM의 파티션 로드 명령을 처리한다.
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
