package transport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sangchul/actorbase/internal/domain"
	pb "github.com/sangchul/actorbase/internal/transport/proto"
	"github.com/sangchul/actorbase/provider"
)

// ── ConnPool ─────────────────────────────────────────────────────────────────

// ConnPool은 주소별 gRPC 커넥션을 캐싱한다.
// SDK가 라우팅 테이블 갱신으로 새 PS 노드에 접속할 때 사용한다.
type ConnPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

// NewConnPool은 빈 ConnPool을 생성한다.
func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]*grpc.ClientConn)}
}

// Get은 addr에 대한 커넥션을 반환한다. 없으면 새로 생성한다.
func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// double-check
	if conn, ok = p.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

// Close는 모든 커넥션을 닫는다.
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

// PSClient는 Partition Server로 Actor 요청을 전송한다.
// SDK가 사용한다.
type PSClient struct {
	conn   *grpc.ClientConn
	client pb.PartitionServiceClient
	codec  provider.Codec
}

// NewPSClient는 PSClient를 생성한다.
func NewPSClient(conn *grpc.ClientConn, codec provider.Codec) *PSClient {
	return &PSClient{
		conn:   conn,
		client: pb.NewPartitionServiceClient(conn),
		codec:  codec,
	}
}

// Send는 partitionID의 Actor에게 req를 전달하고 respPtr에 응답을 역직렬화한다.
// payload 직렬화/역직렬화는 Codec이 담당한다.
// gRPC status error는 provider error로 변환하여 반환한다.
func (c *PSClient) Send(ctx context.Context, actorType, partitionID string, req any, respPtr any) error { //nolint:unparam
	payload, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.client.Send(ctx, &pb.SendRequest{
		PartitionId: partitionID,
		Payload:     payload,
		ActorType:   actorType,
	})
	if err != nil {
		return fromGRPCStatus(err)
	}
	return c.codec.Unmarshal(resp.Payload, respPtr)
}

// Scan은 partitionID의 Actor에게 scan 요청을 전달하고 respPtr에 응답을 역직렬화한다.
// expectedStart/expectedEnd는 SDK가 알고 있는 파티션 key range다.
// PS의 실제 range와 다르면 ErrPartitionMoved를 반환하여 stale 라우팅을 감지한다.
func (c *PSClient) Scan(ctx context.Context, actorType, partitionID string, req any, respPtr any, expectedStart, expectedEnd string) error {
	payload, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.client.Scan(ctx, &pb.ScanRequest{
		PartitionId:            partitionID,
		Payload:                payload,
		ActorType:              actorType,
		ExpectedKeyRangeStart:  expectedStart,
		ExpectedKeyRangeEnd:    expectedEnd,
	})
	if err != nil {
		return fromGRPCStatus(err)
	}
	return c.codec.Unmarshal(resp.Payload, respPtr)
}

// ── PMClient (SDK/abctl → PM, management plane) ──────────────────────────────

// PMClient는 Partition Manager와 통신한다.
// SDK: WatchRouting으로 라우팅 테이블 수신.
// abctl: RequestSplit / RequestMigrate 호출.
type PMClient struct {
	conn   *grpc.ClientConn
	client pb.PartitionManagerServiceClient
}

// NewPMClient는 PMClient를 생성한다.
func NewPMClient(conn *grpc.ClientConn) *PMClient {
	return &PMClient{
		conn:   conn,
		client: pb.NewPartitionManagerServiceClient(conn),
	}
}

// WatchRouting은 라우팅 테이블 변경 채널을 반환한다.
// 연결 직후 현재 테이블을 즉시 전달한다.
// 스트림이 끊기면 자동으로 재연결 후 재구독한다.
// ctx 취소 시 채널이 닫힌다.
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

// RequestSplit은 파티션 split을 PM에 요청한다.
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

// RequestMigrate는 파티션 migration을 PM에 요청한다.
func (c *PMClient) RequestMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
	_, err := c.client.RequestMigrate(ctx, &pb.MigrateRequest{
		PartitionId:  partitionID,
		TargetNodeId: targetNodeID,
		ActorType:    actorType,
	})
	return fromGRPCStatus(err)
}

// MemberInfo는 PS 노드 정보를 담는다.
type MemberInfo struct {
	NodeID  string
	Address string
	Status  domain.NodeStatus
}

// ListMembers는 PM에서 현재 등록된 PS 노드 목록을 조회한다.
func (c *PMClient) ListMembers(ctx context.Context) ([]MemberInfo, error) {
	resp, err := c.client.ListMembers(ctx, &pb.ListMembersRequest{})
	if err != nil {
		return nil, fromGRPCStatus(err)
	}
	members := make([]MemberInfo, len(resp.Members))
	for i, m := range resp.Members {
		var status domain.NodeStatus
		if m.Status == pb.NodeStatus_NODE_STATUS_DRAINING {
			status = domain.NodeStatusDraining
		}
		members[i] = MemberInfo{
			NodeID:  m.NodeId,
			Address: m.Address,
			Status:  status,
		}
	}
	return members, nil
}

// ApplyPolicy는 PM에 YAML 정책을 전송하여 AutoPolicy를 활성화한다.
func (c *PMClient) ApplyPolicy(ctx context.Context, yamlStr string) error {
	_, err := c.client.ApplyPolicy(ctx, &pb.ApplyPolicyRequest{PolicyYaml: yamlStr})
	return fromGRPCStatus(err)
}

// GetPolicy는 PM에서 현재 적용 중인 정책 YAML을 조회한다.
// active=false이면 ManualPolicy 상태.
func (c *PMClient) GetPolicy(ctx context.Context) (yamlStr string, active bool, err error) {
	resp, rpcErr := c.client.GetPolicy(ctx, &pb.GetPolicyRequest{})
	if rpcErr != nil {
		return "", false, fromGRPCStatus(rpcErr)
	}
	return resp.PolicyYaml, resp.Active, nil
}

// ClearPolicy는 PM의 AutoPolicy를 제거하고 ManualPolicy로 전환한다.
func (c *PMClient) ClearPolicy(ctx context.Context) error {
	_, err := c.client.ClearPolicy(ctx, &pb.ClearPolicyRequest{})
	return fromGRPCStatus(err)
}

// GetClusterStats는 PM에서 클러스터 전체(또는 특정 노드)의 통계를 조회한다.
// nodeID가 빈 문자열이면 모든 노드를 반환한다.
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

// ── PSControlClient (PM → PS, control plane) ─────────────────────────────────

// PSControlClient는 PM이 PS에게 split/migrate를 명령하는 데 사용한다.
type PSControlClient struct {
	conn   *grpc.ClientConn
	client pb.PartitionControlServiceClient
}

// NewPSControlClient는 PSControlClient를 생성한다.
func NewPSControlClient(conn *grpc.ClientConn) *PSControlClient {
	return &PSControlClient{
		conn:   conn,
		client: pb.NewPartitionControlServiceClient(conn),
	}
}

// ExecuteSplit은 PS에게 파티션 split을 명령한다.
// splitKey가 ""이면 PS가 SplitHinter 또는 midpoint로 결정한다.
// 실제 사용된 splitKey를 반환한다.
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

// ExecuteMigrateOut은 PS에게 파티션을 대상 노드로 이동시키도록 명령한다.
func (c *PSControlClient) ExecuteMigrateOut(ctx context.Context, actorType, partitionID, targetNodeID, targetAddr string) error {
	_, err := c.client.ExecuteMigrateOut(ctx, &pb.ExecuteMigrateOutRequest{
		PartitionId:   partitionID,
		TargetNodeId:  targetNodeID,
		TargetAddress: targetAddr,
		ActorType:     actorType,
	})
	return fromGRPCStatus(err)
}

// PreparePartition은 target PS에게 파티션을 CheckpointStore에서 로드하도록 명령한다.
func (c *PSControlClient) PreparePartition(ctx context.Context, actorType, partitionID, keyRangeStart, keyRangeEnd string) error {
	_, err := c.client.PreparePartition(ctx, &pb.PreparePartitionRequest{
		PartitionId:   partitionID,
		KeyRangeStart: keyRangeStart,
		KeyRangeEnd:   keyRangeEnd,
		ActorType:     actorType,
	})
	return fromGRPCStatus(err)
}

// NodeStats는 PS 노드 하나의 통계.
type NodeStats struct {
	NodeID         string
	NodeAddr       string
	NodeRPS        float64
	PartitionCount int32
	Partitions     []PartitionStats
}

// PartitionStats는 파티션 하나의 통계.
type PartitionStats struct {
	PartitionID string
	ActorType   string
	KeyCount    int64
	RPS         float64
}

// GetStats는 PS에서 노드 전체 통계를 조회한다.
func (c *PSControlClient) GetStats(ctx context.Context) (*pb.GetStatsResponse, error) {
	return c.client.GetStats(ctx, &pb.GetStatsRequest{})
}

// ── 변환 헬퍼 ────────────────────────────────────────────────────────────────

func protoToRoutingTable(proto *pb.RoutingTableProto) (*domain.RoutingTable, error) {
	entries := make([]domain.RouteEntry, len(proto.Entries))
	for i, e := range proto.Entries {
		var nodeStatus domain.NodeStatus
		if e.NodeStatus == pb.NodeStatus_NODE_STATUS_DRAINING {
			nodeStatus = domain.NodeStatusDraining
		}
		entries[i] = domain.RouteEntry{
			Partition: domain.Partition{
				ID:        e.PartitionId,
				ActorType: e.ActorType,
				KeyRange:  domain.KeyRange{Start: e.KeyRangeStart, End: e.KeyRangeEnd},
			},
			Node: domain.NodeInfo{
				ID:      e.NodeId,
				Address: e.NodeAddress,
				Status:  nodeStatus,
			},
		}
	}
	return domain.NewRoutingTable(proto.Version, entries)
}

// RoutingTableToProto는 domain.RoutingTable을 proto 메시지로 변환한다.
// ps/, pm/의 핸들러에서 사용한다.
func RoutingTableToProto(rt *domain.RoutingTable) *pb.RoutingTableProto {
	entries := rt.Entries()
	protoEntries := make([]*pb.RouteEntryProto, len(entries))
	for i, e := range entries {
		var nodeStatus pb.NodeStatus
		if e.Node.Status == domain.NodeStatusDraining {
			nodeStatus = pb.NodeStatus_NODE_STATUS_DRAINING
		}
		protoEntries[i] = &pb.RouteEntryProto{
			PartitionId:   e.Partition.ID,
			ActorType:     e.Partition.ActorType,
			KeyRangeStart: e.Partition.KeyRange.Start,
			KeyRangeEnd:   e.Partition.KeyRange.End,
			NodeId:        e.Node.ID,
			NodeAddress:   e.Node.Address,
			NodeStatus:    nodeStatus,
		}
	}
	return &pb.RoutingTableProto{
		Version: rt.Version(),
		Entries: protoEntries,
	}
}

// ToGRPCStatus는 provider error를 gRPC status error로 변환한다.
// ps/, pm/ 핸들러에서 사용한다.
var ToGRPCStatus = toGRPCStatus
