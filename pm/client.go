package pm

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sangchul/actorbase/internal/transport"
)

// ── 공개 타입 ─────────────────────────────────────────────────────────────────

// Member는 클러스터에 등록된 PS 노드 정보.
type Member struct {
	NodeID  string
	Address string
	Status  string // "active" | "draining"
}

// RoutingEntry는 라우팅 테이블의 단일 항목.
type RoutingEntry struct {
	PartitionID   string
	ActorType     string
	KeyRangeStart string
	KeyRangeEnd   string
	NodeID        string
	NodeAddr      string
}

// RoutingSnapshot은 특정 시점의 라우팅 테이블 스냅샷.
type RoutingSnapshot struct {
	Version int64
	Entries []RoutingEntry
}

// NodeStat은 PS 노드 하나의 통계.
type NodeStat struct {
	NodeID         string
	NodeAddr       string
	NodeRPS        float64
	PartitionCount int32
	Partitions     []PartitionStat
}

// PartitionStat은 파티션 하나의 통계.
type PartitionStat struct {
	PartitionID string
	ActorType   string
	KeyCount    int64
	RPS         float64
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client는 PM 관리 플레인 클라이언트.
// abctl 같은 운영 도구가 PM에 명령을 보낼 때 사용한다.
type Client struct {
	inner *transport.PMClient
	conn  *grpc.ClientConn
}

// NewClient는 PM 주소로 Client를 생성한다.
func NewClient(pmAddr string) (*Client, error) {
	conn, err := grpc.NewClient(pmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{inner: transport.NewPMClient(conn), conn: conn}, nil
}

// Close는 gRPC 연결을 닫는다.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ListMembers는 현재 등록된 PS 노드 목록을 반환한다.
func (c *Client) ListMembers(ctx context.Context) ([]Member, error) {
	members, err := c.inner.ListMembers(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Member, len(members))
	for i, m := range members {
		status := "active"
		if m.Status != 0 {
			status = "draining"
		}
		result[i] = Member{NodeID: m.NodeID, Address: m.Address, Status: status}
	}
	return result, nil
}

// WatchRouting은 라우팅 테이블 변경을 스트리밍으로 수신한다.
// 채널은 ctx 취소 시 닫힌다.
func (c *Client) WatchRouting(ctx context.Context, clientID string) <-chan RoutingSnapshot {
	raw := c.inner.WatchRouting(ctx, clientID)
	out := make(chan RoutingSnapshot, 1)
	go func() {
		defer close(out)
		for rt := range raw {
			if rt == nil {
				continue
			}
			entries := make([]RoutingEntry, 0, len(rt.Entries()))
			for _, e := range rt.Entries() {
				entries = append(entries, RoutingEntry{
					PartitionID:   e.Partition.ID,
					ActorType:     e.Partition.ActorType,
					KeyRangeStart: e.Partition.KeyRange.Start,
					KeyRangeEnd:   e.Partition.KeyRange.End,
					NodeID:        e.Node.ID,
					NodeAddr:      e.Node.Address,
				})
			}
			snap := RoutingSnapshot{Version: int64(rt.Version()), Entries: entries}
			select {
			case out <- snap:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// RequestSplit은 파티션을 splitKey 기준으로 분할한다.
// 새로 생성된 파티션 ID를 반환한다.
func (c *Client) RequestSplit(ctx context.Context, actorType, partitionID, splitKey string) (string, error) {
	return c.inner.RequestSplit(ctx, actorType, partitionID, splitKey)
}

// RequestMigrate는 파티션을 targetNodeID로 이동한다.
func (c *Client) RequestMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
	return c.inner.RequestMigrate(ctx, actorType, partitionID, targetNodeID)
}

// RequestMerge는 인접한 두 파티션의 merge를 요청한다.
// lower 파티션이 upper 파티션의 상태를 흡수한다.
func (c *Client) RequestMerge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error {
	return c.inner.RequestMerge(ctx, actorType, lowerPartitionID, upperPartitionID)
}

// ApplyPolicy는 YAML 정책을 PM에 적용하여 AutoPolicy를 활성화한다.
func (c *Client) ApplyPolicy(ctx context.Context, yamlStr string) error {
	return c.inner.ApplyPolicy(ctx, yamlStr)
}

// GetPolicy는 현재 적용 중인 정책 YAML과 활성 여부를 반환한다.
func (c *Client) GetPolicy(ctx context.Context) (yamlStr string, active bool, err error) {
	return c.inner.GetPolicy(ctx)
}

// ClearPolicy는 AutoPolicy를 제거하고 기본 정책으로 복귀한다.
func (c *Client) ClearPolicy(ctx context.Context) error {
	return c.inner.ClearPolicy(ctx)
}

// GetClusterStats는 클러스터 전체(또는 특정 노드)의 통계를 반환한다.
// nodeID가 빈 문자열이면 모든 노드를 반환한다.
func (c *Client) GetClusterStats(ctx context.Context, nodeID string) ([]NodeStat, error) {
	stats, err := c.inner.GetClusterStats(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	result := make([]NodeStat, len(stats))
	for i, n := range stats {
		parts := make([]PartitionStat, len(n.Partitions))
		for j, p := range n.Partitions {
			parts[j] = PartitionStat{
				PartitionID: p.PartitionID,
				ActorType:   p.ActorType,
				KeyCount:    p.KeyCount,
				RPS:         float64(p.RPS),
			}
		}
		result[i] = NodeStat{
			NodeID:         n.NodeID,
			NodeAddr:       n.NodeAddr,
			NodeRPS:        float64(n.NodeRPS),
			PartitionCount: int32(n.PartitionCount),
			Partitions:     parts,
		}
	}
	return result, nil
}
