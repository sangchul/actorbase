package policy

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sangchul/actorbase/provider"
)

// ── Common configuration types ─────────────────────────────────────────────────────────────

// RunnerConfig holds parameters required to run a balancerRunner.
// It carries execution interval and cooldown settings that are independent of the algorithm.
type RunnerConfig struct {
	CheckInterval time.Duration
	Cooldown      CooldownConfig
}

// CooldownConfig holds wait time settings after a split or migrate operation.
type CooldownConfig struct {
	Global    time.Duration `yaml:"global"`    // cluster-wide wait time
	Partition time.Duration `yaml:"partition"` // per-partition wait time
}

// BalanceConfig defines the criteria for moving partitions between nodes. It is shared across multiple algorithms.
type BalanceConfig struct {
	MaxPartitionDiff int     `yaml:"max_partition_diff"` // allowed difference in partition count between nodes
	RPSImbalancePct  float64 `yaml:"rps_imbalance_pct"`  // allowed RPS difference between nodes in percent
}

// ── ParsePolicy ────────────────────────────────────────────────────────────────

// ParsePolicy parses YAML bytes and returns a BalancePolicy and RunnerConfig.
// It selects the implementation based on the algorithm field value. To add a new algorithm, only add a case to this function.
func ParsePolicy(data []byte) (provider.BalancePolicy, *RunnerConfig, error) {
	var header struct {
		Algorithm     string         `yaml:"algorithm"`
		CheckInterval time.Duration  `yaml:"check_interval"`
		Cooldown      CooldownConfig `yaml:"cooldown"`
	}
	if err := yaml.Unmarshal(data, &header); err != nil {
		return nil, nil, fmt.Errorf("policy: parse YAML: %w", err)
	}
	if header.CheckInterval <= 0 {
		return nil, nil, fmt.Errorf("policy: check_interval must be positive")
	}
	runnerCfg := &RunnerConfig{
		CheckInterval: header.CheckInterval,
		Cooldown:      header.Cooldown,
	}

	switch header.Algorithm {
	case "threshold":
		var cfg ThresholdConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, nil, fmt.Errorf("policy: parse YAML: %w", err)
		}
		return NewThresholdPolicy(&cfg), runnerCfg, nil
	case "relative":
		var cfg RelativeConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, nil, fmt.Errorf("policy: parse YAML: %w", err)
		}
		return NewRelativePolicy(&cfg), runnerCfg, nil
	default:
		return nil, nil, fmt.Errorf("policy: unsupported algorithm %q (supported: threshold, relative)", header.Algorithm)
	}
}

// ── Shared helpers ─────────────────────────────────────────────────────────────────

// pickLeastLoaded returns the node ID with the fewest partitions among reachable nodes, excluding excludeNodeID.
func pickLeastLoaded(stats provider.ClusterStats, excludeNodeID string) string {
	best := ""
	bestCount := -1
	for _, ns := range stats.Nodes {
		if !ns.Reachable || ns.Node.ID == excludeNodeID {
			continue
		}
		if bestCount < 0 || len(ns.Partitions) < bestCount {
			bestCount = len(ns.Partitions)
			best = ns.Node.ID
		}
	}
	return best
}
