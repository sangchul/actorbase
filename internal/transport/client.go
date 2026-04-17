package transport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sangchul/actorbase/internal/domain"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/provider"
)

// ── ConnPool ─────────────────────────────────────────────────────────────────

// ConnPool caches gRPC connections by address.
// Used by the SDK when connecting to new PS nodes after a routing table update.
type ConnPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

// NewConnPool creates an empty ConnPool.
func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]*grpc.ClientConn)}
}

// Get returns the connection for addr, creating one if it does not exist.
// If the cached connection is in TransientFailure or Shutdown state, it is
// replaced with a fresh connection so that a restarted server is reachable.
func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		state := conn.GetState()
		if state != connectivity.TransientFailure && state != connectivity.Shutdown {
			return conn, nil
		}
		// Stale connection: log and fall through to replace it.
		slog.Info("transport: ConnPool: replacing stale connection", "addr", addr, "state", state)
		_ = conn.Close()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// Double-check: another goroutine may have replaced it already.
	if conn, ok = p.conns[addr]; ok {
		state := conn.GetState()
		if state != connectivity.TransientFailure && state != connectivity.Shutdown {
			return conn, nil
		}
		_ = conn.Close()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

// Close closes all connections.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}

// ── PSClient (SDK → PS, data plane) ─────────────────────────────────────────

// PSClient sends Actor requests to a Partition Server.
// Used by the SDK.
type PSClient struct {
	conn   *grpc.ClientConn
	client pb.PartitionServiceClient
	codec  provider.Codec
}

// NewPSClient creates a PSClient.
func NewPSClient(conn *grpc.ClientConn, codec provider.Codec) *PSClient {
	return &PSClient{
		conn:   conn,
		client: pb.NewPartitionServiceClient(conn),
		codec:  codec,
	}
}

// Send delivers req to the Actor for partitionID and deserializes the response
// into respPtr. Payload serialization/deserialization is handled by the Codec.
// epoch is the RouteEntry.Epoch known to the caller; PS uses it to reject writes
// from clients with a stale routing table (split-brain fencing).
// gRPC status errors are converted to provider errors before returning.
func (c *PSClient) Send(ctx context.Context, actorType, partitionID string, epoch uint64, req any, respPtr any) error { //nolint:unparam
	payload, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.client.Send(ctx, &pb.SendRequest{
		PartitionId: partitionID,
		Payload:     payload,
		ActorType:   actorType,
		Epoch:       epoch,
	})
	if err != nil {
		return fromGRPCStatus(err)
	}
	return c.codec.Unmarshal(resp.Payload, respPtr)
}

// Scan delivers a scan request to the Actor for partitionID and deserializes
// the response into respPtr. expectedStart/expectedEnd are the partition key
// range known to the SDK. If they differ from the PS's actual range,
// ErrPartitionMoved is returned to signal a stale routing entry.
// epoch is the RouteEntry.Epoch for split-brain fencing (same semantics as Send).
func (c *PSClient) Scan(ctx context.Context, actorType, partitionID string, epoch uint64, req any, respPtr any, expectedStart, expectedEnd string) error {
	payload, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.client.Scan(ctx, &pb.ScanRequest{
		PartitionId:           partitionID,
		Payload:               payload,
		ActorType:             actorType,
		ExpectedKeyRangeStart: expectedStart,
		ExpectedKeyRangeEnd:   expectedEnd,
		Epoch:                 epoch,
	})
	if err != nil {
		return fromGRPCStatus(err)
	}
	return c.codec.Unmarshal(resp.Payload, respPtr)
}

// ── PMClient (SDK/abctl → PM, management plane) ──────────────────────────────

// PMClient communicates with the Partition Manager.
// SDK: receives routing table updates via WatchRouting.
// abctl: calls RequestSplit / RequestMigrate.
type PMClient struct {
	conn   *grpc.ClientConn
	client pb.PartitionManagerServiceClient
}

// NewPMClient creates a PMClient.
func NewPMClient(conn *grpc.ClientConn) *PMClient {
	return &PMClient{
		conn:   conn,
		client: pb.NewPartitionManagerServiceClient(conn),
	}
}

// WatchRouting returns a channel of routing table changes.
// The current table is delivered immediately upon connection.
// The stream reconnects automatically when interrupted.
// The channel is closed when ctx is cancelled.
func (c *PMClient) WatchRouting(ctx context.Context, clientID string) <-chan *domain.RoutingTable {
	ch := make(chan *domain.RoutingTable, 8)
	go c.watchRoutingLoop(ctx, clientID, ch)
	return ch
}

func (c *PMClient) watchRoutingLoop(ctx context.Context, clientID string, ch chan *domain.RoutingTable) {
	defer close(ch)

	const retryDelay = 2 * time.Second
	for {
		if err := c.streamRouting(ctx, clientID, ch); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("WatchRouting stream error, retrying", "err", err, "delay", retryDelay)
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *PMClient) streamRouting(ctx context.Context, clientID string, ch chan *domain.RoutingTable) error {
	stream, err := c.client.WatchRouting(ctx, &pb.WatchRoutingRequest{ClientId: clientID})
	if err != nil {
		return err
	}
	for {
		proto, err := stream.Recv()
		if err != nil {
			return err
		}
		rt, err := protoToRoutingTable(proto)
		if err != nil {
			slog.Error("WatchRouting: unmarshal routing table", "err", err)
			continue
		}
		select {
		case ch <- rt:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// RequestSplit requests a partition split from the PM.
func (c *PMClient) RequestSplit(ctx context.Context, actorType, partitionID, splitKey string) (string, error) {
	resp, err := c.client.RequestSplit(ctx, &pb.SplitRequest{
		PartitionId: partitionID,
		SplitKey:    splitKey,
		ActorType:   actorType,
	})
	if err != nil {
		return "", fromGRPCStatus(err)
	}
	return resp.NewPartitionId, nil
}

// RequestMigrate requests a partition migration from the PM.
func (c *PMClient) RequestMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
	_, err := c.client.RequestMigrate(ctx, &pb.MigrateRequest{
		PartitionId:  partitionID,
		TargetNodeId: targetNodeID,
		ActorType:    actorType,
	})
	return fromGRPCStatus(err)
}

// RequestMerge requests a merge of two adjacent partitions from the PM.
func (c *PMClient) RequestMerge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error {
	_, err := c.client.RequestMerge(ctx, &pb.MergeRequest{
		ActorType:        actorType,
		LowerPartitionId: lowerPartitionID,
		UpperPartitionId: upperPartitionID,
	})
	return fromGRPCStatus(err)
}

// MemberInfo holds information about a PS node.
type MemberInfo struct {
	NodeID  string
	Address string
	Status  domain.NodeStatus
}

// ListMembers retrieves all nodes in the catalog (all states) from the PM.
func (c *PMClient) ListMembers(ctx context.Context) ([]MemberInfo, error) {
	resp, err := c.client.ListMembers(ctx, &pb.ListMembersRequest{})
	if err != nil {
		return nil, fromGRPCStatus(err)
	}
	members := make([]MemberInfo, len(resp.Members))
	for i, m := range resp.Members {
		members[i] = MemberInfo{
			NodeID:  m.NodeId,
			Address: m.Address,
			Status:  protoStatusToDomain(m.Status),
		}
	}
	return members, nil
}

// RequestJoin asks the PM to admit this node into the cluster.
// The node must be pre-registered with Waiting status via AddNode.
// actorTypes lists the actor types this PS supports; PM validates them against cfg.ActorTypes.
func (c *PMClient) RequestJoin(ctx context.Context, nodeID, addr string, actorTypes []string) error {
	_, err := c.client.RequestJoin(ctx, &pb.RequestJoinRequest{NodeId: nodeID, Address: addr, ActorTypes: actorTypes})
	return fromGRPCStatus(err)
}

// SetNodeDraining notifies the PM that this node is beginning graceful shutdown.
func (c *PMClient) SetNodeDraining(ctx context.Context, nodeID string) error {
	_, err := c.client.SetNodeDraining(ctx, &pb.SetNodeDrainingRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// AddNode pre-registers a node in the catalog with Waiting status.
func (c *PMClient) AddNode(ctx context.Context, nodeID, addr string) error {
	_, err := c.client.AddNode(ctx, &pb.AddNodeRequest{NodeId: nodeID, Address: addr})
	return fromGRPCStatus(err)
}

// RemoveNode deletes a Waiting or Failed node from the catalog.
func (c *PMClient) RemoveNode(ctx context.Context, nodeID string) error {
	_, err := c.client.RemoveNode(ctx, &pb.RemoveNodeRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// ResetNode transitions a Failed node back to Waiting.
func (c *PMClient) ResetNode(ctx context.Context, nodeID string) error {
	_, err := c.client.ResetNode(ctx, &pb.ResetNodeRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// SetNodeDrained notifies the PM that drain has completed. Draining → Drained.
func (c *PMClient) SetNodeDrained(ctx context.Context, nodeID string) error {
	_, err := c.client.SetNodeDrained(ctx, &pb.SetNodeDrainedRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// ActivateNode transitions a Drained node back to Active.
func (c *PMClient) ActivateNode(ctx context.Context, nodeID string) error {
	_, err := c.client.ActivateNode(ctx, &pb.ActivateNodeRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// RestrictNode transitions an Active node to Restricted (no new partitions).
func (c *PMClient) RestrictNode(ctx context.Context, nodeID string) error {
	_, err := c.client.RestrictNode(ctx, &pb.RestrictNodeRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// UnrestrictNode transitions a Restricted node back to Active.
func (c *PMClient) UnrestrictNode(ctx context.Context, nodeID string) error {
	_, err := c.client.UnrestrictNode(ctx, &pb.UnrestrictNodeRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// protoStatusToDomain converts pb.NodeStatus to domain.NodeStatus.
func protoStatusToDomain(s pb.NodeStatus) domain.NodeStatus {
	switch s {
	case pb.NodeStatus_NODE_STATUS_ACTIVE:
		return domain.NodeStatusActive
	case pb.NodeStatus_NODE_STATUS_DRAINING:
		return domain.NodeStatusDraining
	case pb.NodeStatus_NODE_STATUS_FAILED:
		return domain.NodeStatusFailed
	case pb.NodeStatus_NODE_STATUS_DRAINED:
		return domain.NodeStatusDrained
	case pb.NodeStatus_NODE_STATUS_RESTRICTED:
		return domain.NodeStatusRestricted
	default:
		return domain.NodeStatusWaiting
	}
}

// ApplyPolicy sends a YAML policy to the PM to activate AutoPolicy.
func (c *PMClient) ApplyPolicy(ctx context.Context, yamlStr string) error {
	_, err := c.client.ApplyPolicy(ctx, &pb.ApplyPolicyRequest{PolicyYaml: yamlStr})
	return fromGRPCStatus(err)
}

// GetPolicy retrieves the currently applied policy YAML from the PM.
// active=false indicates that the PM is in ManualPolicy mode.
func (c *PMClient) GetPolicy(ctx context.Context) (yamlStr string, active bool, err error) {
	resp, rpcErr := c.client.GetPolicy(ctx, &pb.GetPolicyRequest{})
	if rpcErr != nil {
		return "", false, fromGRPCStatus(rpcErr)
	}
	return resp.PolicyYaml, resp.Active, nil
}

// ClearPolicy removes the AutoPolicy from the PM and switches to ManualPolicy.
func (c *PMClient) ClearPolicy(ctx context.Context) error {
	_, err := c.client.ClearPolicy(ctx, &pb.ClearPolicyRequest{})
	return fromGRPCStatus(err)
}

// GetClusterStats retrieves statistics for the entire cluster (or a specific
// node) from the PM. An empty nodeID returns all nodes.
func (c *PMClient) GetClusterStats(ctx context.Context, nodeID string) ([]NodeStats, error) {
	resp, err := c.client.GetClusterStats(ctx, &pb.GetClusterStatsRequest{NodeId: nodeID})
	if err != nil {
		return nil, fromGRPCStatus(err)
	}
	result := make([]NodeStats, len(resp.Nodes))
	for i, n := range resp.Nodes {
		partitions := make([]PartitionStats, len(n.Partitions))
		for j, p := range n.Partitions {
			partitions[j] = PartitionStats{
				PartitionID: p.PartitionId,
				ActorType:   p.ActorType,
				KeyCount:    p.KeyCount,
				RPS:         p.Rps,
			}
		}
		result[i] = NodeStats{
			NodeID:         n.NodeId,
			NodeAddr:       n.NodeAddr,
			NodeRPS:        n.NodeRps,
			PartitionCount: n.PartitionCount,
			Partitions:     partitions,
		}
	}
	return result, nil
}

// GetQueueStatus retrieves the PM task queue state: running task, pending
// tasks, and recent history.
func (c *PMClient) GetQueueStatus(ctx context.Context) (*pb.GetQueueStatusResponse, error) {
	resp, err := c.client.GetQueueStatus(ctx, &pb.GetQueueStatusRequest{})
	if err != nil {
		return nil, fromGRPCStatus(err)
	}
	return resp, nil
}

// Heartbeat sends a liveness signal to the PM.
// Called periodically by PS; PM uses the last-seen timestamp to detect failures.
func (c *PMClient) Heartbeat(ctx context.Context, nodeID string) error {
	_, err := c.client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// EvictionComplete signals to PM that this PS has finished flushing all WAL/checkpoints.
// Allows PM to skip walFlushMargin and immediately assign the partition to a new PS.
func (c *PMClient) EvictionComplete(ctx context.Context, nodeID string) error {
	_, err := c.client.EvictionComplete(ctx, &pb.EvictionCompleteRequest{NodeId: nodeID})
	return fromGRPCStatus(err)
}

// ── PSController interface (PM → PS, control plane) ──────────────────────────

// PSController is the interface for PM → PS control-plane operations.
// Using the interface allows tests to inject mock implementations without
// requiring a real gRPC connection.
type PSController interface {
	ExecuteSplit(ctx context.Context, actorType, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error)
	ExecuteMigrateOut(ctx context.Context, actorType, partitionID, targetNodeID, targetAddr string) error
	PreparePartition(ctx context.Context, actorType, partitionID, keyRangeStart, keyRangeEnd string, epoch uint64) error
	ExecuteMerge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error
	GetStats(ctx context.Context) (*pb.GetStatsResponse, error)
	Ping(ctx context.Context) error
}

// PSClientFactory creates PSController instances for a given PS address.
// The production implementation wraps ConnPool; tests provide a mock.
type PSClientFactory interface {
	GetClient(addr string) (PSController, error)
}

// NewConnPoolFactory returns a PSClientFactory backed by pool.
func NewConnPoolFactory(pool *ConnPool) PSClientFactory {
	return &connPoolFactory{pool: pool}
}

type connPoolFactory struct{ pool *ConnPool }

func (f *connPoolFactory) GetClient(addr string) (PSController, error) {
	conn, err := f.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return NewPSControlClient(conn), nil
}

// ── PSControlClient (PM → PS, control plane) ─────────────────────────────────

// PSControlClient is used by the PM to issue split/migrate commands to a PS.
// It implements PSController.
type PSControlClient struct {
	conn   *grpc.ClientConn
	client pb.PartitionControlServiceClient
}

// NewPSControlClient creates a PSControlClient.
func NewPSControlClient(conn *grpc.ClientConn) *PSControlClient {
	return &PSControlClient{
		conn:   conn,
		client: pb.NewPartitionControlServiceClient(conn),
	}
}

// ExecuteSplit instructs the PS to split a partition.
// If splitKey is "", the PS determines it via SplitHinter or midpoint.
// Returns the splitKey that was actually used.
func (c *PSControlClient) ExecuteSplit(ctx context.Context, actorType, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error) {
	resp, err := c.client.ExecuteSplit(ctx, &pb.ExecuteSplitRequest{
		PartitionId:    partitionID,
		SplitKey:       splitKey,
		KeyRangeStart:  keyRangeStart,
		KeyRangeEnd:    keyRangeEnd,
		NewPartitionId: newPartitionID,
		ActorType:      actorType,
	})
	if err != nil {
		return "", fromGRPCStatus(err)
	}
	return resp.SplitKey, nil
}

// ExecuteMigrateOut instructs the PS to move a partition to the target node.
func (c *PSControlClient) ExecuteMigrateOut(ctx context.Context, actorType, partitionID, targetNodeID, targetAddr string) error {
	_, err := c.client.ExecuteMigrateOut(ctx, &pb.ExecuteMigrateOutRequest{
		PartitionId:   partitionID,
		TargetNodeId:  targetNodeID,
		TargetAddress: targetAddr,
		ActorType:     actorType,
	})
	return fromGRPCStatus(err)
}

// PreparePartition instructs the target PS to load a partition from the CheckpointStore.
// epoch is the assignment epoch issued by PM; PS stores it for future control-command validation.
// Uses WaitForReady so that a freshly restarted PS (IDLE connection) is reached even
// when the gRPC client's reconnect backoff has not yet elapsed.
func (c *PSControlClient) PreparePartition(ctx context.Context, actorType, partitionID, keyRangeStart, keyRangeEnd string, epoch uint64) error {
	slog.Info("transport: PreparePartition → PS", "partition", partitionID, "actor_type", actorType,
		"epoch", epoch, "conn_state", c.conn.GetState())
	_, err := c.client.PreparePartition(ctx, &pb.PreparePartitionRequest{
		PartitionId:   partitionID,
		KeyRangeStart: keyRangeStart,
		KeyRangeEnd:   keyRangeEnd,
		ActorType:     actorType,
		Epoch:         epoch,
	}, grpc.WaitForReady(true))
	if err != nil {
		slog.Error("transport: PreparePartition RPC error", "partition", partitionID,
			"raw_err", err, "conn_state", c.conn.GetState())
	}
	return fromGRPCStatus(err)
}

// NodeStats holds statistics for a single PS node.
type NodeStats struct {
	NodeID         string
	NodeAddr       string
	NodeRPS        float64
	PartitionCount int32
	Partitions     []PartitionStats
}

// PartitionStats holds statistics for a single partition.
type PartitionStats struct {
	PartitionID string
	ActorType   string
	KeyCount    int64
	RPS         float64
}

// ExecuteMerge instructs the PS to merge two partitions.
// The lower partition absorbs the state of the upper partition.
func (c *PSControlClient) ExecuteMerge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error {
	_, err := c.client.ExecuteMerge(ctx, &pb.ExecuteMergeRequest{
		ActorType:        actorType,
		LowerPartitionId: lowerPartitionID,
		UpperPartitionId: upperPartitionID,
	})
	return fromGRPCStatus(err)
}

// GetStats retrieves overall node statistics from the PS.
func (c *PSControlClient) GetStats(ctx context.Context) (*pb.GetStatsResponse, error) {
	return c.client.GetStats(ctx, &pb.GetStatsRequest{})
}

// Ping checks whether the PS is alive. Used by PM after lease expiry to detect false positives.
func (c *PSControlClient) Ping(ctx context.Context) error {
	_, err := c.client.Ping(ctx, &pb.PingRequest{})
	return fromGRPCStatus(err)
}

// ── Conversion helpers ────────────────────────────────────────────────────────

func protoToRoutingTable(proto *pb.RoutingTableProto) (*domain.RoutingTable, error) {
	entries := make([]domain.RouteEntry, len(proto.Entries))
	nodeAddrs := make(map[string]string, len(proto.Entries))
	for i, e := range proto.Entries {
		entries[i] = domain.RouteEntry{
			Partition: domain.Partition{
				ID:        e.PartitionId,
				ActorType: e.ActorType,
				KeyRange:  domain.KeyRange{Start: e.KeyRangeStart, End: e.KeyRangeEnd},
			},
			NodeID: e.NodeId,
			Epoch:  e.Epoch,
		}
		if e.NodeAddress != "" {
			nodeAddrs[e.NodeId] = e.NodeAddress
		}
	}
	rt, err := domain.NewRoutingTable(proto.Version, entries)
	if err != nil {
		return nil, err
	}
	return rt.WithNodeAddrs(nodeAddrs), nil
}

// RoutingTableToProto converts a domain.RoutingTable to a proto message.
// Used by handlers in ps/ and pm/.
func RoutingTableToProto(rt *domain.RoutingTable) *pb.RoutingTableProto {
	entries := rt.Entries()
	protoEntries := make([]*pb.RouteEntryProto, len(entries))
	for i, e := range entries {
		addr, _ := rt.NodeAddress(e.NodeID)
		protoEntries[i] = &pb.RouteEntryProto{
			PartitionId:   e.Partition.ID,
			ActorType:     e.Partition.ActorType,
			KeyRangeStart: e.Partition.KeyRange.Start,
			KeyRangeEnd:   e.Partition.KeyRange.End,
			NodeId:        e.NodeID,
			NodeAddress:   addr,
			NodeStatus:    pb.NodeStatus_NODE_STATUS_WAITING, // NodeStatus managed by NodeCatalog
			Epoch:         e.Epoch,
		}
	}
	return &pb.RoutingTableProto{
		Version: rt.Version(),
		Entries: protoEntries,
	}
}

// ToGRPCStatus converts a provider error to a gRPC status error.
// Used by handlers in ps/ and pm/.
var ToGRPCStatus = toGRPCStatus
