package pm

import (
	"fmt"
	"time"

	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
)

const (
	defaultPingTimeout      = 2 * time.Second
	defaultHeartbeatTimeout = 5 * time.Second
	defaultWalFlushMargin   = 3 * time.Second
)

// Config holds all settings and dependencies required to create a PM.
type Config struct {
	// ─── Required (provided by the user) ─────────────────────────

	ListenAddr    string   // gRPC listen address ("host:port").
	EtcdEndpoints []string // List of etcd endpoints.
	// RedisAddr is the Redis address for routing table and policy storage (e.g. "localhost:6379").
	// If empty, etcd is used for both routing table and policy (no Redis required).
	RedisAddr string

	// ActorTypes is the list of actor types to create during bootstrap.
	// When the first PS registers, an initial partition covering the full key range is created per actor type.
	// At least one type must be specified.
	ActorTypes []string

	// HTTPAddr is the address for the web console HTTP server (e.g. ":8080").
	// If empty, the web console is not started.
	HTTPAddr string

	// ─── Optional (have defaults) ────────────────────────────────

	Metrics provider.Metrics // If nil, a no-op implementation is used.

	// PingTimeout is the timeout for the liveness ping sent to a PS after lease expiry.
	// If the PS responds within this duration, the lease expiry is treated as a false positive.
	// Default: 2s.
	PingTimeout time.Duration

	// BalancePolicy is the load-balancing strategy implementation.
	// If nil, NoopBalancePolicy (does nothing) is used.
	// Users can inject their own provider.BalancePolicy implementation,
	// or apply a ThresholdPolicy at runtime via "abctl policy apply".
	BalancePolicy provider.BalancePolicy

	// HeartbeatTimeout is how long PM waits for a heartbeat before declaring a PS dead.
	// Default: 5s.
	HeartbeatTimeout time.Duration

	// WalFlushMargin is the extra wait after declaring a PS dead before sending
	// PreparePartition to the replacement PS (used when EvictionComplete is not received).
	// Gives the dead PS time to finish flushing its WAL to shared storage.
	// Default: 3s.
	WalFlushMargin time.Duration
}

func (c *Config) setDefaults() {
	if c.BalancePolicy == nil {
		c.BalancePolicy = &policy.NoopBalancePolicy{}
	}
	if c.PingTimeout <= 0 {
		c.PingTimeout = defaultPingTimeout
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if c.WalFlushMargin <= 0 {
		c.WalFlushMargin = defaultWalFlushMargin
	}
}

func (c *Config) validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("pm: ListenAddr is required")
	}
	if len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("pm: EtcdEndpoints is required")
	}
	if len(c.ActorTypes) == 0 {
		return fmt.Errorf("pm: ActorTypes is required (at least one actor type)")
	}
	return nil
}
