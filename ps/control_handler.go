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

// controlHandler는 PartitionControlService gRPC 핸들러.
// PM → PS control plane 요청을 처리한다.
// req.ActorType으로 올바른 actorDispatcher를 선택한다.
type controlHandler struct {
	pb.UnimplementedPartitionControlServiceServer

	dispatchers map[string]actorDispatcher
	nodeID      string
	routing     *atomic.Pointer[domain.RoutingTable]
}

// ExecuteSplit은 PM의 split 명령을 처리한다.
// req.SplitKey가 비어 있으면 Actor의 SplitHint() 또는 key range midpoint로 결정한다.
// 실제 사용된 split key를 응답에 포함한다.
func (h *controlHandler) ExecuteSplit(
	ctx context.Context,
	req *pb.ExecuteSplitRequest,
) (*pb.ExecuteSplitResponse, error) {
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}

	// req에 key range가 없으면 routing table에서 조회 (midpoint fallback 보장)
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

// engineMidpoint는 engine.KeyRangeMidpoint의 별칭. controlHandler가 직접 호출할 일은 없으나
// 패키지 참조 유지용으로 선언한다.
var _ = engine.KeyRangeMidpoint

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

// ExecuteMerge는 PM의 merge 명령을 처리한다.
// lower 파티션이 upper 파티션의 상태를 흡수한다.
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

// GetStats는 PM의 통계 조회 요청을 처리한다.
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
