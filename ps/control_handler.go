package ps

import (
	"context"

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
	if err := h.host.Split(ctx, req.PartitionId, req.SplitKey, req.NewPartitionId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.ExecuteSplitResponse{}, nil
}

// ExecuteMigrateOut은 PM의 migration out 명령을 처리한다.
// Actor의 최종 checkpoint를 저장하고 evict한다.
func (h *controlHandler[Req, Resp]) ExecuteMigrateOut(
	ctx context.Context,
	req *pb.ExecuteMigrateOutRequest,
) (*pb.ExecuteMigrateOutResponse, error) {
	if err := h.host.Evict(ctx, req.PartitionId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.ExecuteMigrateOutResponse{}, nil
}

// PreparePartition은 PM의 파티션 로드 명령을 처리한다.
// CheckpointStore에서 파티션을 로드하여 활성화한다.
func (h *controlHandler[Req, Resp]) PreparePartition(
	ctx context.Context,
	req *pb.PreparePartitionRequest,
) (*pb.PreparePartitionResponse, error) {
	if err := h.host.Activate(ctx, req.PartitionId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.PreparePartitionResponse{}, nil
}
