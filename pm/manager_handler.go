package pm

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/policy"
)

// managerHandler is the PartitionManagerService gRPC handler.
// Handles SDK/abctl → PM management-plane requests.
type managerHandler struct {
	pb.UnimplementedPartitionManagerServiceServer
	server *Server
}

// WatchRouting sends the current routing table immediately upon connection,
// then pushes updates via streaming whenever the table changes.
func (h *managerHandler) WatchRouting(
	req *pb.WatchRoutingRequest,
	stream pb.PartitionManagerService_WatchRoutingServer,
) error {
	sub := &subscriber{notify: make(chan struct{}, 1)}
	h.server.subscribe(req.ClientId, sub)
	defer h.server.unsubscribe(req.ClientId)

	// Deliver the current routing table immediately.
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

// RequestSplit requests splitting partitionID at the given splitKey.
// Rejects manual commands while AutoPolicy is active.
func (h *managerHandler) RequestSplit(
	ctx context.Context,
	req *pb.SplitRequest,
) (*pb.SplitResponse, error) {
	if h.server.isAutoActive() {
		return nil, status.Error(codes.PermissionDenied,
			"manual split not allowed while AutoPolicy is active (use abctl policy clear to disable)")
	}
	h.server.opMu.Lock()
	defer h.server.opMu.Unlock()

	newPartitionID, err := h.server.doSplit(ctx, req.ActorType, req.PartitionId, req.SplitKey)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.SplitResponse{NewPartitionId: newPartitionID}, nil
}

// RequestMigrate requests moving partitionID to targetNodeID.
// Rejects manual commands while AutoPolicy is active.
func (h *managerHandler) RequestMigrate(
	ctx context.Context,
	req *pb.MigrateRequest,
) (*pb.MigrateResponse, error) {
	if h.server.isAutoActive() {
		return nil, status.Error(codes.PermissionDenied,
			"manual migrate not allowed while AutoPolicy is active (use abctl policy clear to disable)")
	}
	h.server.opMu.Lock()
	defer h.server.opMu.Unlock()

	if err := h.server.doMigrate(ctx, req.ActorType, req.PartitionId, req.TargetNodeId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.MigrateResponse{}, nil
}

// RequestMerge requests merging two adjacent partitions.
// Rejects manual commands while AutoPolicy is active.
func (h *managerHandler) RequestMerge(
	ctx context.Context,
	req *pb.MergeRequest,
) (*pb.MergeResponse, error) {
	if h.server.isAutoActive() {
		return nil, status.Error(codes.PermissionDenied,
			"manual merge not allowed while AutoPolicy is active (use abctl policy clear to disable)")
	}
	h.server.opMu.Lock()
	defer h.server.opMu.Unlock()

	if err := h.server.doMerge(ctx, req.ActorType, req.LowerPartitionId, req.UpperPartitionId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.MergeResponse{}, nil
}

// ApplyPolicy applies a YAML policy to the PM, activating AutoPolicy.
func (h *managerHandler) ApplyPolicy(
	ctx context.Context,
	req *pb.ApplyPolicyRequest,
) (*pb.ApplyPolicyResponse, error) {
	pol, runnerCfg, err := policy.ParsePolicy([]byte(req.PolicyYaml))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid policy: %v", err)
	}
	if err := h.server.applyPolicy(ctx, req.PolicyYaml, pol, runnerCfg); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.ApplyPolicyResponse{}, nil
}

// GetPolicy returns the YAML of the currently applied policy.
func (h *managerHandler) GetPolicy(
	_ context.Context,
	_ *pb.GetPolicyRequest,
) (*pb.GetPolicyResponse, error) {
	h.server.policyMu.RLock()
	yamlStr := h.server.activePolicyYAML
	active := h.server.activePolicy != nil
	h.server.policyMu.RUnlock()
	return &pb.GetPolicyResponse{PolicyYaml: yamlStr, Active: active}, nil
}

// ClearPolicy removes the active policy and reverts to manual policy.
func (h *managerHandler) ClearPolicy(
	ctx context.Context,
	_ *pb.ClearPolicyRequest,
) (*pb.ClearPolicyResponse, error) {
	if err := h.server.clearPolicy(ctx); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.ClearPolicyResponse{}, nil
}

// GetClusterStats returns statistics for the entire cluster (or a specific node).
// The PM calls each PS's GetStats RPC in parallel and aggregates the results.
// Only Active nodes are queried (Waiting/Failed nodes have no running gRPC server).
func (h *managerHandler) GetClusterStats(
	ctx context.Context,
	req *pb.GetClusterStatsRequest,
) (*pb.GetClusterStatsResponse, error) {
	allNodes, err := h.server.nodeCatalog.ListNodes(ctx)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}

	// Only include Active nodes for stats (they have a live gRPC server).
	var nodes []domain.NodeInfo
	for _, n := range allNodes {
		if n.Status != domain.NodeStatusActive {
			continue
		}
		if req.NodeId != "" && n.ID != req.NodeId {
			continue
		}
		nodes = append(nodes, n)
	}

	pool := transport.NewConnPool()
	defer pool.Close() //nolint:errcheck

	type nodeResult struct {
		nodeID   string
		nodeAddr string
		resp     *pb.GetStatsResponse
		err      error
	}

	results := make([]nodeResult, len(nodes))
	var wg sync.WaitGroup
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, node domain.NodeInfo) {
			defer wg.Done()
			results[idx] = nodeResult{nodeID: node.ID, nodeAddr: node.Address}
			conn, connErr := pool.Get(node.Address)
			if connErr != nil {
				results[idx].err = connErr
				return
			}
			psCtrl := transport.NewPSControlClient(conn)
			resp, statsErr := psCtrl.GetStats(fetchCtx)
			results[idx].resp = resp
			results[idx].err = statsErr
		}(i, n)
	}
	wg.Wait()

	nodeProtos := make([]*pb.NodeStatsProto, 0, len(results))
	for _, r := range results {
		if r.err != nil || r.resp == nil {
			continue
		}
		nodeProtos = append(nodeProtos, &pb.NodeStatsProto{
			NodeId:         r.nodeID,
			NodeAddr:       r.nodeAddr,
			NodeRps:        r.resp.NodeRps,
			PartitionCount: r.resp.PartitionCount,
			Partitions:     r.resp.Partitions,
		})
	}
	return &pb.GetClusterStatsResponse{Nodes: nodeProtos}, nil
}

// ListMembers returns all nodes in the catalog across all states.
func (h *managerHandler) ListMembers(
	ctx context.Context,
	_ *pb.ListMembersRequest,
) (*pb.ListMembersResponse, error) {
	nodes, err := h.server.nodeCatalog.ListNodes(ctx)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	members := make([]*pb.MemberInfo, len(nodes))
	for i, n := range nodes {
		members[i] = &pb.MemberInfo{
			NodeId:  n.ID,
			Address: n.Address,
			Status:  domainStatusToProto(n.Status),
		}
	}
	return &pb.ListMembersResponse{Members: members}, nil
}

// RequestJoin is called by PS on startup to request cluster membership.
// PM validates that the node is in Waiting state and transitions it to Active.
func (h *managerHandler) RequestJoin(
	ctx context.Context,
	req *pb.RequestJoinRequest,
) (*pb.RequestJoinResponse, error) {
	entry, found, err := h.server.nodeCatalog.GetNode(ctx, req.NodeId)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	if !found {
		return nil, status.Errorf(codes.PermissionDenied,
			"node %q is not registered; run 'abctl node add' first", req.NodeId)
	}
	if entry.Status != domain.NodeStatusWaiting {
		return nil, status.Errorf(codes.PermissionDenied,
			"node %q is in %v state, expected Waiting", req.NodeId, entry.Status)
	}
	if err := h.server.nodeCatalog.UpdateStatus(ctx, req.NodeId, domain.NodeStatusActive); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.RequestJoinResponse{}, nil
}

// SetNodeDraining is called by PS before graceful shutdown.
// PM transitions the node from Active to Draining.
func (h *managerHandler) SetNodeDraining(
	ctx context.Context,
	req *pb.SetNodeDrainingRequest,
) (*pb.SetNodeDrainingResponse, error) {
	entry, found, err := h.server.nodeCatalog.GetNode(ctx, req.NodeId)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
	}
	if entry.Status != domain.NodeStatusActive {
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %q is in %v state, expected Active", req.NodeId, entry.Status)
	}
	if err := h.server.nodeCatalog.UpdateStatus(ctx, req.NodeId, domain.NodeStatusDraining); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.SetNodeDrainingResponse{}, nil
}

// AddNode pre-registers a new node in the catalog with Waiting status.
// Called by abctl node add.
func (h *managerHandler) AddNode(
	ctx context.Context,
	req *pb.AddNodeRequest,
) (*pb.AddNodeResponse, error) {
	node := domain.NodeInfo{
		ID:      req.NodeId,
		Address: req.Address,
		Status:  domain.NodeStatusWaiting,
	}
	if err := h.server.nodeCatalog.AddNode(ctx, node); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.AddNodeResponse{}, nil
}

// RemoveNode deletes a node from the catalog.
// Only Waiting or Failed nodes may be removed.
func (h *managerHandler) RemoveNode(
	ctx context.Context,
	req *pb.RemoveNodeRequest,
) (*pb.RemoveNodeResponse, error) {
	entry, found, err := h.server.nodeCatalog.GetNode(ctx, req.NodeId)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
	}
	if entry.Status != domain.NodeStatusWaiting && entry.Status != domain.NodeStatusFailed {
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %q is in %v state; only Waiting or Failed nodes may be removed", req.NodeId, entry.Status)
	}
	if err := h.server.nodeCatalog.RemoveNode(ctx, req.NodeId); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.RemoveNodeResponse{}, nil
}

// ResetNode transitions a Failed node back to Waiting, allowing it to rejoin.
// Called by abctl node reset after the operator has diagnosed the failure.
func (h *managerHandler) ResetNode(
	ctx context.Context,
	req *pb.ResetNodeRequest,
) (*pb.ResetNodeResponse, error) {
	entry, found, err := h.server.nodeCatalog.GetNode(ctx, req.NodeId)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
	}
	if entry.Status != domain.NodeStatusFailed {
		return nil, status.Errorf(codes.FailedPrecondition,
			"node %q is in %v state, expected Failed", req.NodeId, entry.Status)
	}
	if err := h.server.nodeCatalog.UpdateStatus(ctx, req.NodeId, domain.NodeStatusWaiting); err != nil {
		return nil, transport.ToGRPCStatus(err)
	}
	return &pb.ResetNodeResponse{}, nil
}

// domainStatusToProto converts domain.NodeStatus to pb.NodeStatus.
func domainStatusToProto(s domain.NodeStatus) pb.NodeStatus {
	switch s {
	case domain.NodeStatusWaiting:
		return pb.NodeStatus_NODE_STATUS_WAITING
	case domain.NodeStatusActive:
		return pb.NodeStatus_NODE_STATUS_ACTIVE
	case domain.NodeStatusDraining:
		return pb.NodeStatus_NODE_STATUS_DRAINING
	case domain.NodeStatusFailed:
		return pb.NodeStatus_NODE_STATUS_FAILED
	default:
		return pb.NodeStatus_NODE_STATUS_WAITING
	}
}
