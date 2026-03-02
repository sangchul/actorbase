package ps

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/engine"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
	"github.com/oomymy/actorbase/internal/transport"
	"github.com/oomymy/actorbase/provider"
)

// partitionHandler는 PartitionService gRPC 핸들러.
// SDK → PS data plane 요청을 처리한다.
type partitionHandler[Req, Resp any] struct {
	pb.UnimplementedPartitionServiceServer

	host    *engine.ActorHost[Req, Resp]
	routing *atomic.Pointer[domain.RoutingTable]
	codec   provider.Codec
	nodeID  string
}

// Send는 partitionID의 Actor에 요청을 전달하고 응답을 반환한다.
func (h *partitionHandler[Req, Resp]) Send(
	ctx context.Context,
	req *pb.SendRequest,
) (*pb.SendResponse, error) {
	// 1. 라우팅 테이블 조회
	rt := h.routing.Load()
	if rt == nil {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 2. 파티션 존재 확인
	entry, ok := rt.LookupByPartition(req.PartitionId)
	if !ok {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 3. 이 PS가 파티션을 소유하는지 확인
	if entry.Node.ID != h.nodeID {
		return nil, status.Error(codes.Unavailable, provider.ErrPartitionNotOwned.Error())
	}

	// 4. 파티션이 Draining 상태이면 거부
	if entry.PartitionStatus == domain.PartitionStatusDraining {
		return nil, status.Error(codes.ResourceExhausted, provider.ErrPartitionBusy.Error())
	}

	// 5. payload 역직렬화
	var userReq Req
	if err := h.codec.Unmarshal(req.Payload, &userReq); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal request: %v", err)
	}

	// 6. Actor에 전달
	resp, err := h.host.Send(ctx, req.PartitionId, userReq)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}

	// 7. 응답 직렬화
	payload, err := h.codec.Marshal(resp)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal response: %v", err)
	}

	return &pb.SendResponse{Payload: payload}, nil
}
