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

	newPartitionID, err := h.server.splitter.Split(ctx, req.ActorType, req.PartitionId, req.SplitKey)
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

	if err := h.server.migrator.Migrate(ctx, req.ActorType, req.PartitionId, req.TargetNodeId); err != nil {
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

	if err := h.server.merger.Merge(ctx, req.ActorType, req.LowerPartitionId, req.UpperPartitionId); err != nil {
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
func (h *managerHandler) GetClusterStats(
	ctx context.Context,
	req *pb.GetClusterStatsRequest,
) (*pb.GetClusterStatsResponse, error) {
	nodes, err := h.server.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return nil, transport.ToGRPCStatus(err)
	}

	// Filter by node_id if specified.
	if req.NodeId != "" {
		filtered := nodes[:0]
		for _, n := range nodes {
			if n.ID == req.NodeId {
				filtered = append(filtered, n)
				break
			}
		}
		nodes = filtered
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

// ListMembers returns the list of currently registered PS nodes.
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
