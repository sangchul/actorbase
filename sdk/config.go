package sdk

import (
	"fmt"
	"os"
	"time"

	"github.com/sangchul/actorbase/provider"
)

const (
	defaultMaxRetries    = 3
	defaultRetryInterval = 100 * time.Millisecond
)

// Config holds the settings required to create a Client.
type Config[Req, Resp any] struct {
	// ─── Required (provided by the user) ───────────────────────────────────────

	// PMAddr: single PM mode. Required when EtcdEndpoints is not set.
	PMAddr string
	// EtcdEndpoints: HA mode. When set, the leader PM address is automatically resolved from etcd.
	// On PM failure, automatically rediscovers the new leader and restores the connection.
	// Either PMAddr or EtcdEndpoints must be set, not both.
	EtcdEndpoints []string

	TypeID string         // identifier for the actor type this Client targets
	Codec  provider.Codec // must be the same implementation injected into the PS

	// ─── Optional (have defaults) ───────────────────────────────────────

	ClientID      string        // identifier for debugging and logging. default: hostname
	MaxRetries    int           // maximum number of retries. default: 3
	RetryInterval time.Duration // wait time before each retry. default: 100ms
}

func (c *Config[Req, Resp]) setDefaults() {
	if c.ClientID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "sdk-client"
		}
		c.ClientID = hostname
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = defaultRetryInterval
	}
}

func (c *Config[Req, Resp]) validate() error {
	if c.PMAddr == "" && len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("sdk: PMAddr or EtcdEndpoints is required")
	}
	if c.TypeID == "" {
		return fmt.Errorf("sdk: TypeID is required")
	}
	if c.Codec == nil {
		return fmt.Errorf("sdk: Codec is required")
	}
	return nil
}

// haMode returns whether the client is in etcd-based HA mode.
func (c *Config[Req, Resp]) haMode() bool {
	return len(c.EtcdEndpoints) > 0
}
