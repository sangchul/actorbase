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

// partitionHandler는 PartitionService gRPC 핸들러.
// SDK → PS data plane 요청을 처리한다.
// req.ActorType으로 올바른 actorDispatcher를 선택한다.
type partitionHandler struct {
	pb.UnimplementedPartitionServiceServer

	dispatchers map[string]actorDispatcher
	routing     *atomic.Pointer[domain.RoutingTable]
	nodeID      string
}

// Send는 req.ActorType의 Actor에 요청을 전달하고 응답을 반환한다.
func (h *partitionHandler) Send(
	ctx context.Context,
	req *pb.SendRequest,
) (*pb.SendResponse, error) {
	// 1. actor type에 맞는 dispatcher 조회
	d, ok := h.dispatchers[req.ActorType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown actor type: %s", req.ActorType)
	}

	// 2. 라우팅 테이블 조회
	rt := h.routing.Load()
	if rt == nil {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 3. 파티션 존재 확인 및 소유 검증
	entry, ok := rt.LookupByPartition(req.PartitionId)
	if !ok {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}
	if entry.Node.ID != h.nodeID {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 4. 파티션이 Draining 상태이면 거부
	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return nil, status.Error(codes.ResourceExhausted, provider.ErrPartitionBusy.Error())
	}

	// 5. dispatcher를 통해 Actor에 전달 (역직렬화 포함)
	payload, err := d.Send(ctx, req.PartitionId, req.Payload)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}

	return &pb.SendResponse{Payload: payload}, nil
}

// Scan은 SDK의 범위 조회 요청을 Actor.Receive()로 전달한다.
// Send와 동일하지만 expected key range를 검증하여 stale 라우팅을 감지한다.
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

	// 파티션 key range가 SDK의 기대값과 다르면 라우팅 테이블이 stale한 것 (파티션이 split됨)
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
