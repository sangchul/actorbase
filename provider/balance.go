package provider

import "context"

// ── Type definitions ────────────────────────────────────────────────────────────

// NodeStatus represents the status of a node.
type NodeStatus int

const (
	NodeStatusActive   NodeStatus = iota
	NodeStatusDraining            // Partition migration in progress (graceful shutdown)
)

// NodeInfo holds cluster node information.
type NodeInfo struct {
	ID     string
	Addr   string
	Status NodeStatus
}

// NodeLeaveReason indicates the reason a node left the cluster.
type NodeLeaveReason int

const (
	// NodeLeaveGraceful is a normal shutdown. drainPartitions has completed partition migration.
	NodeLeaveGraceful NodeLeaveReason = iota
	// NodeLeaveFailure is a failure (SIGKILL, network disconnect, etc.). Detected via etcd lease expiry.
	NodeLeaveFailure
)

// PartitionInfo holds the state and statistics of a single partition.
// Composed by combining GetStats (RPS, KeyCount) and the routing table (KeyRange, NodeID).
type PartitionInfo struct {
	PartitionID   string
	ActorType     string
	KeyCount      int64   // -1: Actor does not implement Countable
	RPS           float64 // 60-second sliding window average
	KeyRangeStart string
	KeyRangeEnd   string
}

// NodeStats holds the state of a single node.
type NodeStats struct {
	Node       NodeInfo
	Reachable  bool            // false means GetStats failed; Partitions are based on the routing table
	NodeRPS    float64         // 0 if Reachable is false
	Partitions []PartitionInfo
}

// ClusterStats is a snapshot of cluster state passed to BalancePolicy method calls.
type ClusterStats struct {
	Nodes []NodeStats
}

// ── BalanceAction ───────────────────────────────────────────────────────────────

// BalanceActionType is the kind of rebalance operation to execute.
type BalanceActionType int

const (
	// ActionSplit splits a partition at the given SplitKey.
	ActionSplit BalanceActionType = iota
	// ActionMigrate moves a partition from a live PS to another node.
	ActionMigrate
	// ActionFailover recovers a partition from a failed node.
	// Since the source PS is dead, only PreparePartition is performed (ExecuteMigrateOut is skipped).
	ActionFailover
	// ActionMerge merges two adjacent partitions into one.
	// PartitionID is the lower partition; MergeTarget is the upper partition.
	ActionMerge
)

// BalanceAction represents a single rebalance operation to be executed by the PM.
type BalanceAction struct {
	Type        BalanceActionType
	ActorType   string
	PartitionID string
	TargetNode  string // Used by ActionMigrate / ActionFailover only
	MergeTarget string // Used by ActionMerge only — the upper partition to absorb
}

// ── BalancePolicy interface ────────────────────────────────────────────────────

// BalancePolicy defines the distribution strategy interface for the PM.
//
// Users can implement it directly, or load a built-in implementation (ThresholdPolicy)
// from pm/policy via YAML. Inject into pm.Config.BalancePolicy for the PM to use.
//
// The PM executes the returned []BalanceAction in order.
// Returning nil or an empty slice results in no action.
type BalancePolicy interface {
	// Evaluate is called every check_interval.
	// Returns split/migrate actions based on the current cluster stats.
	Evaluate(ctx context.Context, stats ClusterStats) []BalanceAction

	// OnNodeJoined is called when a new node joins the cluster.
	// May return migrate actions to redistribute load.
	OnNodeJoined(ctx context.Context, node NodeInfo, stats ClusterStats) []BalanceAction

	// OnNodeLeft is called when a node leaves the cluster.
	// The reason parameter distinguishes between graceful shutdown and failure.
	//
	// Graceful: drainPartitions has already completed migration, so this is usually a no-op.
	//
	// Failure: The source PS is dead, so ActionFailover must be returned to recover partitions.
	//   The stats include the dead node's partitions (routing-table based, Reachable=false).
	OnNodeLeft(ctx context.Context, node NodeInfo, reason NodeLeaveReason, stats ClusterStats) []BalanceAction
}
