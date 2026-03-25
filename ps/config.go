package ps

import (
	"fmt"
	"time"

	"github.com/sangchul/actorbase/provider"
)

const (
	defaultIdleTimeout            = 5 * time.Minute
	defaultEvictInterval          = 1 * time.Minute
	defaultCheckpointInterval     = 1 * time.Minute
	defaultCheckpointWALThreshold = 100
	defaultEtcdLeaseTTL           = 10 * time.Second
	defaultDrainTimeout           = 60 * time.Second
	defaultShutdownTimeout        = 30 * time.Second
)

// BaseConfig holds settings shared across the entire PS instance.
type BaseConfig struct {
	// ─── Required (provided by the user) ─────────────────────────

	NodeID        string
	Addr          string   // gRPC listen address ("host:port"). Registered in etcd so other nodes can connect.
	EtcdEndpoints []string // List of etcd endpoints.

	// ─── Optional (have defaults) ────────────────────────────────

	Metrics provider.Metrics // If nil, metrics collection is skipped.

	EtcdLeaseTTL time.Duration // Node lease TTL. Default: 10s.

	// EvictionScheduler settings (shared across all actor types)
	IdleTimeout   time.Duration // Evict an actor if it receives no messages for this duration. Default: 5m.
	EvictInterval time.Duration // Interval between eviction checks. Default: 1m.

	// CheckpointScheduler settings (shared across all actor types)
	CheckpointInterval time.Duration // Interval for periodic checkpoints. Default: 1m.

	// Graceful shutdown settings
	DrainTimeout    time.Duration // Maximum time to wait for partition pre-migration. Default: 60s.
	ShutdownTimeout time.Duration // Maximum time to wait for EvictAll. Default: 30s.
}

func (c *BaseConfig) setDefaults() {
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.EvictInterval <= 0 {
		c.EvictInterval = defaultEvictInterval
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = defaultCheckpointInterval
	}
	if c.EtcdLeaseTTL <= 0 {
		c.EtcdLeaseTTL = defaultEtcdLeaseTTL
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = defaultDrainTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
}

func (c *BaseConfig) validate() error {
	if c.NodeID == "" {
		return errorf("NodeID is required")
	}
	if c.Addr == "" {
		return errorf("Addr is required")
	}
	if len(c.EtcdEndpoints) == 0 {
		return errorf("EtcdEndpoints is required")
	}
	return nil
}

// TypeConfig holds the configuration for a specific actor type.
// Identified by TypeID; multiple actor types can be registered on the same PS.
type TypeConfig[Req, Resp any] struct {
	// ─── Required (provided by the user) ─────────────────────────

	TypeID          string                           // Actor type identifier. Must match the routing table.
	Factory         provider.ActorFactory[Req, Resp] // Function that creates Actor instances.
	Codec           provider.Codec                   // Inject the same Codec implementation used by the SDK.
	WALStore        provider.WALStore                // WAL storage backend.
	CheckpointStore provider.CheckpointStore         // Checkpoint storage backend.

	// ─── Optional (have defaults) ────────────────────────────────

	MailboxSize            int           // Default: uses the engine default.
	FlushSize              int           // Maximum WAL batch size. Default: uses the engine default.
	FlushInterval          time.Duration // Maximum wait time for a WAL batch. Default: uses the engine default.
	CheckpointWALThreshold int           // Trigger an automatic checkpoint after this many WAL entries. Default: 100.
}

func (c *TypeConfig[Req, Resp]) validate() error {
	if c.TypeID == "" {
		return errorf("TypeID is required")
	}
	if c.Factory == nil {
		return errorf("Factory is required for type %q", c.TypeID)
	}
	if c.Codec == nil {
		return errorf("Codec is required for type %q", c.TypeID)
	}
	if c.WALStore == nil {
		return errorf("WALStore is required for type %q", c.TypeID)
	}
	if c.CheckpointStore == nil {
		return errorf("CheckpointStore is required for type %q", c.TypeID)
	}
	return nil
}

func errorf(format string, args ...any) error {
	return fmt.Errorf("ps: "+format, args...)
}
