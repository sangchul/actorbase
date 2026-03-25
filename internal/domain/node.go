package domain

// NodeStatus represents the current state of a node.
type NodeStatus int

const (
	// NodeStatusActive: the node is operating normally and accepting requests.
	NodeStatusActive NodeStatus = iota

	// NodeStatusDraining: partition migration is in progress.
	// The node still accepts new requests but will soon relinquish its partitions.
	NodeStatusDraining
)

// NodeInfo holds metadata for a single node in the cluster.
type NodeInfo struct {
	ID      string     // unique node identifier within the cluster
	Address string     // gRPC connection address ("host:port")
	Status  NodeStatus
}
