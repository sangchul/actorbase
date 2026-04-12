package domain

// NodeStatus represents the current state of a node.
type NodeStatus int

const (
	// NodeStatusWaiting: pre-registered by the PM but not yet online.
	// A PS must be in this state before it can call RequestJoin.
	NodeStatusWaiting NodeStatus = iota

	// NodeStatusActive: the node is online and accepting requests.
	NodeStatusActive

	// NodeStatusDraining: graceful shutdown in progress.
	// The node has announced its intent to shut down and is migrating its partitions.
	NodeStatusDraining

	// NodeStatusFailed: unexpected failure (SIGKILL or lease TTL expiry).
	// The PM will failover the node's partitions but will NOT automatically
	// transition back to Waiting. An operator must run 'abctl node reset'
	// after diagnosing the failure.
	NodeStatusFailed

	// NodeStatusDrained: drain completed successfully. The PS process may still
	// be running (with no partitions). The operator can call 'abctl node activate'
	// to return it to Active, or let it exit naturally (lease expiry → Waiting).
	NodeStatusDrained

	// NodeStatusRestricted: online and serving its current partitions, but will NOT
	// accept new partitions as a migration target. Useful when a node is being
	// decommissioned gradually or has capacity constraints.
	// Transition back to Active via 'abctl node unrestrict'.
	NodeStatusRestricted
)

// NodeInfo holds metadata for a single node in the cluster.
type NodeInfo struct {
	ID      string     // unique node identifier within the cluster
	Address string     // gRPC connection address ("host:port")
	Status  NodeStatus
}
