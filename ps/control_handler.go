package ps

import (
	"context"
	"log/slog"

	"github.com/oomymy/actorbase/internal/engine"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
	"github.com/oomymy/actorbase/internal/transport"
)

// controlHandler는 PartitionControlService gRPC 핸들러.
// PM → PS control plane 요청을 처리한다.
type controlHandler[Req, Resp any] struct {
	pb.UnimplementedPartitionControlServiceServer

	host   *engine.ActorHost[Req, Resp]
	nodeID string
}

// ExecuteSplit은 PM의 split 명령을 처리한다.
func (h *controlHandler[Req, Resp]) ExecuteSplit(
	ctx context.Context,
	req *pb.ExecuteSplitRequest,
) (*pb.ExecuteSplitResponse, error) {
	slog.Info("ctrl: ExecuteSplit", "node", h.nodeID, "partition", req.PartitionId, "split_key", req.SplitKey, "new_partition", req.NewPartitionId)
	if err := h.host.Split(ctx, req.PartitionId, req.SplitKey, req.NewPartitionId); err != nil {
		slog.Error("ctrl: ExecuteSplit failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: ExecuteSplit done", "node", h.nodeID, "partition", req.PartitionId)
	return &pb.ExecuteSplitResponse{}, nil
}

// ExecuteMigrateOut은 PM의 migration out 명령을 처리한다.
// Actor의 최종 checkpoint를 저장하고 evict한다.
func (h *controlHandler[Req, Resp]) ExecuteMigrateOut(
	ctx context.Context,
	req *pb.ExecuteMigrateOutRequest,
) (*pb.ExecuteMigrateOutResponse, error) {
	slog.Info("ctrl: ExecuteMigrateOut", "node", h.nodeID, "partition", req.PartitionId, "target", req.TargetNodeId)
	if err := h.host.Evict(ctx, req.PartitionId); err != nil {
		slog.Error("ctrl: ExecuteMigrateOut failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: ExecuteMigrateOut done", "node", h.nodeID, "partition", req.PartitionId)
	return &pb.ExecuteMigrateOutResponse{}, nil
}

// PreparePartition은 PM의 파티션 로드 명령을 처리한다.
// CheckpointStore에서 파티션을 로드하여 활성화한다.
func (h *controlHandler[Req, Resp]) PreparePartition(
	ctx context.Context,
	req *pb.PreparePartitionRequest,
) (*pb.PreparePartitionResponse, error) {
	slog.Info("ctrl: PreparePartition", "node", h.nodeID, "partition", req.PartitionId)
	if err := h.host.Activate(ctx, req.PartitionId); err != nil {
		slog.Error("ctrl: PreparePartition failed", "node", h.nodeID, "partition", req.PartitionId, "err", err)
		return nil, transport.ToGRPCStatus(err)
	}
	slog.Info("ctrl: PreparePartition done", "node", h.nodeID, "partition", req.PartitionId)
	return &pb.PreparePartitionResponse{}, nil
}
