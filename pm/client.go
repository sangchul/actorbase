package pm

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sangchul/actorbase/internal/domain"
	"github.com/sangchul/actorbase/internal/transport"
)

// ── Public types ──────────────────────────────────────────────────────────────

// Member holds information about a PS node registered in the cluster.
type Member struct {
	NodeID  string
	Address string
	Status  domain.NodeStatus
}

// RoutingEntry is a single entry in the routing table.
type RoutingEntry struct {
	PartitionID   string
	ActorType     string
	KeyRangeStart string
	KeyRangeEnd   string
	NodeID        string
	NodeAddr      string
}

// RoutingSnapshot is a point-in-time snapshot of the routing table.
type RoutingSnapshot struct {
	Version int64
	Entries []RoutingEntry
}

// NodeStat holds statistics for a single PS node.
type NodeStat struct {
	NodeID         string
	NodeAddr       string
	NodeRPS        float64
	PartitionCount int32
	Partitions     []PartitionStat
}

// PartitionStat holds statistics for a single partition.
type PartitionStat struct {
	PartitionID string
	ActorType   string
	KeyCount    int64
	RPS         float64
}

// ── Client ────────────────────────────────────────────────────────────────────

// Client is a PM management-plane client.
// Used by operational tools such as abctl to send commands to the PM.
type Client struct {
	inner *transport.PMClient
	conn  *grpc.ClientConn
}

// NewClient creates a Client connected to the given PM address.
func NewClient(pmAddr string) (*Client, error) {
	conn, err := grpc.NewClient(pmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{inner: transport.NewPMClient(conn), conn: conn}, nil
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ListMembers returns all nodes in the catalog across all states.
func (c *Client) ListMembers(ctx context.Context) ([]Member, error) {
	members, err := c.inner.ListMembers(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Member, len(members))
	for i, m := range members {
		result[i] = Member{NodeID: m.NodeID, Address: m.Address, Status: m.Status}
	}
	return result, nil
}

// AddNode pre-registers a node in the catalog with Waiting status.
func (c *Client) AddNode(ctx context.Context, nodeID, addr string) error {
	return c.inner.AddNode(ctx, nodeID, addr)
}

// RemoveNode deletes a Waiting or Failed node from the catalog.
func (c *Client) RemoveNode(ctx context.Context, nodeID string) error {
	return c.inner.RemoveNode(ctx, nodeID)
}

// ResetNode transitions a Failed node back to Waiting.
func (c *Client) ResetNode(ctx context.Context, nodeID string) error {
	return c.inner.ResetNode(ctx, nodeID)
}

// WatchRouting receives routing table changes via streaming.
// The channel is closed when ctx is cancelled.
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

// RequestSplit splits a partition at the given splitKey.
// Returns the newly created partition ID.
func (c *Client) RequestSplit(ctx context.Context, actorType, partitionID, splitKey string) (string, error) {
	return c.inner.RequestSplit(ctx, actorType, partitionID, splitKey)
}

// RequestMigrate moves a partition to targetNodeID.
func (c *Client) RequestMigrate(ctx context.Context, actorType, partitionID, targetNodeID string) error {
	return c.inner.RequestMigrate(ctx, actorType, partitionID, targetNodeID)
}

// RequestMerge requests merging two adjacent partitions.
// The lower partition absorbs the state of the upper partition.
func (c *Client) RequestMerge(ctx context.Context, actorType, lowerPartitionID, upperPartitionID string) error {
	return c.inner.RequestMerge(ctx, actorType, lowerPartitionID, upperPartitionID)
}

// ApplyPolicy applies a YAML policy to the PM, activating AutoPolicy.
func (c *Client) ApplyPolicy(ctx context.Context, yamlStr string) error {
	return c.inner.ApplyPolicy(ctx, yamlStr)
}

// GetPolicy returns the YAML of the currently applied policy and whether it is active.
func (c *Client) GetPolicy(ctx context.Context) (yamlStr string, active bool, err error) {
	return c.inner.GetPolicy(ctx)
}

// ClearPolicy removes AutoPolicy and reverts to the default policy.
func (c *Client) ClearPolicy(ctx context.Context) error {
	return c.inner.ClearPolicy(ctx)
}

// GetClusterStats returns statistics for the entire cluster (or a specific node).
// If nodeID is an empty string, all nodes are returned.
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
