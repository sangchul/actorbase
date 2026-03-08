package pm

import (
	"context"

	"github.com/oomymy/actorbase/internal/domain"
	"github.com/oomymy/actorbase/internal/transport"
	pb "github.com/oomymy/actorbase/internal/transport/proto"
)

// managerHandler는 PartitionManagerService gRPC 핸들러.
// SDK/abctl → PM management plane 요청을 처리한다.
type managerHandler struct {
	pb.UnimplementedPartitionManagerServiceServer
	server *Server
}

// WatchRouting은 연결 즉시 현재 라우팅 테이블을 전송하고,
// 이후 변경 시마다 스트리밍으로 push한다.
func (h *managerHandler) WatchRouting(
	req *pb.WatchRoutingRequest,
	stream pb.PartitionManagerService_WatchRoutingServer,
) error {
	sub := &subscriber{notify: make(chan struct{}, 1)}
	h.server.subscribe(req.ClientId, sub)
	defer h.server.unsubscribe(req.ClientId)

	// 현재 라우팅 테이블 즉시 전달
	if current := h.server.routing.Load(); current != nil {
		if err := stream.Send(transport.RoutingTableToProto(current)); err != nil {
			return err
		}
	}

	for {
		select {
		case <-sub.notify:
			rt := sub.latest.Load()
			if rt == nil {
				continue
			}
			if err := stream.Send(transport.RoutingTableToProto(rt)); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

// RequestSplit은 partitionID를 splitKey 기준으로 분할하도록 요청한다.
func (h *managerHandler) RequestSplit(
	ctx context.Context,
	req *pb.SplitRequest,
) (*pb.SplitResponse, error) {
	h.server.opMu.Lock()
	defer h.server.opMu.Unlock()

	newPartitionID, err := h.server.splitter.Split(ctx, req.ActorType, req.PartitionId, req.SplitKey)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.SplitResponse{NewPartitionId: newPartitionID}, nil
}

// RequestMigrate는 partitionID를 targetNodeID로 이동시키도록 요청한다.
func (h *managerHandler) RequestMigrate(
	ctx context.Context,
	req *pb.MigrateRequest,
) (*pb.MigrateResponse, error) {
	h.server.opMu.Lock()
	defer h.server.opMu.Unlock()

	if err := h.server.migrator.Migrate(ctx, req.ActorType, req.PartitionId, req.TargetNodeId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.MigrateResponse{}, nil
}

// ListMembers는 현재 등록된 PS 노드 목록을 반환한다.
func (h *managerHandler) ListMembers(
	ctx context.Context,
	_ *pb.ListMembersRequest,
) (*pb.ListMembersResponse, error) {
	nodes, err := h.server.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	members := make([]*pb.MemberInfo, len(nodes))
	for i, n := range nodes {
		var status pb.NodeStatus
		if n.Status == domain.NodeStatusDraining {
			status = pb.NodeStatus_NODE_STATUS_DRAINING
		}
		members[i] = &pb.MemberInfo{
			NodeId:  n.ID,
			Address: n.Address,
			Status:  status,
		}
	}
	return &pb.ListMembersResponse{Members: members}, nil
}
