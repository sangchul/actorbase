package policy

import (
	"context"

	"github.com/sangchul/actorbase/provider"
)

// RelativeConfig holds the configuration for the relative algorithm-based automatic split/balance policy (for YAML parsing).
//
// Example YAML:
//
//	algorithm: relative
//	check_interval: 30s
//	cooldown:
//	  global: 60s
//	  partition: 120s
//	split:
//	  rps_multiplier: 3.0
//	  min_avg_rps: 10.0
//	balance:
//	  max_partition_diff: 2
//	  rps_imbalance_pct: 30
type RelativeConfig struct {
	Split   RelativeSplit `yaml:"split"`
	Balance BalanceConfig `yaml:"balance"`
}

// RelativeSplit holds the split configuration for the relative algorithm.
type RelativeSplit struct {
	RPSMultiplier float64 `yaml:"rps_multiplier"` // multiplier relative to average RPS (e.g., 3.0)
	MinAvgRPS     float64 `yaml:"min_avg_rps"`    // minimum average RPS required to consider a split
}

// NewRelativePolicy creates a RelativePolicy from a RelativeConfig.
func NewRelativePolicy(cfg *RelativeConfig) *RelativePolicy {
	return &RelativePolicy{
		SplitRPSMultiplier: cfg.Split.RPSMultiplier,
		MinAvgRPS:          cfg.Split.MinAvgRPS,
		Balance:            cfg.Balance,
	}
}

// RelativePolicy is a BalancePolicy implementation that evaluates split/migrate decisions
// based on relative thresholds compared to the cluster average.
//
// Unlike ThresholdPolicy which uses absolute thresholds (rps > 1000),
// RelativePolicy determines splits by comparing a partition's RPS to a multiple of
// the average RPS across all partitions in the cluster.
//
// Example: SplitRPSMultiplier=3.0, average RPS=200 → only split partitions exceeding 600.
// The threshold automatically lowers during low traffic and rises during high traffic.
// MinAvgRPS prevents unnecessary splits when traffic is too low.
type RelativePolicy struct {
	// SplitRPSMultiplier is the multiplier used as the split threshold.
	// A partition is split when its RPS exceeds (cluster average partition RPS × multiplier).
	// Example: 3.0 → split when a partition exceeds 3x the average.
	SplitRPSMultiplier float64

	// MinAvgRPS is the minimum average RPS required to consider a split.
	// No split occurs when the cluster average partition RPS is below this value.
	// Prevents noise-triggered splits when traffic is extremely low.
	MinAvgRPS float64

	// Balance defines the criteria for moving partitions between nodes.
	// Uses the same configuration structure as ThresholdPolicy.
	Balance BalanceConfig
}

// Evaluate returns split/migrate actions based on relative thresholds compared to the cluster average.
func (p *RelativePolicy) Evaluate(_ context.Context, stats provider.ClusterStats) []provider.BalanceAction {
	// ── split evaluation ──────────────────────────────────────────────────────────────

	// calculate average RPS across all partitions
	var totalRPS float64
	var partitionCount int
	for _, ns := range stats.Nodes {
		if !ns.Reachable {
			continue
		}
		for _, part := range ns.Partitions {
			totalRPS += part.RPS
			partitionCount++
		}
	}

	if partitionCount > 0 {
		avgRPS := totalRPS / float64(partitionCount)

		// search for split candidates. the split key is determined by the PS (via SplitHinter or midpoint).
		if avgRPS >= p.MinAvgRPS && p.SplitRPSMultiplier > 0 {
			threshold := avgRPS * p.SplitRPSMultiplier
			for _, ns := range stats.Nodes {
				if !ns.Reachable {
					continue
				}
				for _, part := range ns.Partitions {
					if part.RPS <= threshold {
						continue
					}
					return []provider.BalanceAction{{
						Type:        provider.ActionSplit,
						ActorType:   part.ActorType,
						PartitionID: part.PartitionID,
					}}
				}
			}
		}
	}

	// ── balance evaluation (same logic as ThresholdPolicy) ───────────────────────────────────

	type summary struct {
		nodeID         string
		partitionCount int
		totalRPS       float64
	}
	var summaries []summary
	for _, ns := range stats.Nodes {
		if !ns.Reachable {
			continue
		}
		var rps float64
		for _, part := range ns.Partitions {
			rps += part.RPS
		}
		summaries = append(summaries, summary{
			nodeID:         ns.Node.ID,
			partitionCount: len(ns.Partitions),
			totalRPS:       rps,
		})
	}
	if len(summaries) < 2 {
		return nil
	}

	maxIdx, minIdx := 0, 0
	for i, s := range summaries {
		if s.partitionCount > summaries[maxIdx].partitionCount {
			maxIdx = i
		}
		if s.partitionCount < summaries[minIdx].partitionCount {
			minIdx = i
		}
	}
	maxNode := summaries[maxIdx]
	minNode := summaries[minIdx]

	partDiff := maxNode.partitionCount - minNode.partitionCount
	rpsImbalance := false
	if maxNode.totalRPS > 0 {
		pct := (maxNode.totalRPS - minNode.totalRPS) / maxNode.totalRPS * 100
		rpsImbalance = pct > p.Balance.RPSImbalancePct
	}
	if partDiff <= p.Balance.MaxPartitionDiff && !rpsImbalance {
		return nil
	}

	for _, ns := range stats.Nodes {
		if !ns.Reachable || ns.Node.ID != maxNode.nodeID || len(ns.Partitions) == 0 {
			continue
		}
		return []provider.BalanceAction{{
			Type:        provider.ActionMigrate,
			ActorType:   ns.Partitions[0].ActorType,
			PartitionID: ns.Partitions[0].PartitionID,
			TargetNode:  minNode.nodeID,
		}}
	}
	return nil
}

// OnNodeJoined migrates one partition from the node with the most partitions when a new node joins.
func (p *RelativePolicy) OnNodeJoined(_ context.Context, newNode provider.NodeInfo, stats provider.ClusterStats) []provider.BalanceAction {
	var maxCount int
	var migratable *provider.PartitionInfo

	for _, ns := range stats.Nodes {
		if !ns.Reachable || ns.Node.ID == newNode.ID {
			continue
		}
		if len(ns.Partitions) > maxCount {
			maxCount = len(ns.Partitions)
			part := ns.Partitions[0]
			migratable = &part
		}
	}
	if migratable == nil || maxCount <= 1 {
		return nil
	}
	return []provider.BalanceAction{{
		Type:        provider.ActionMigrate,
		ActorType:   migratable.ActorType,
		PartitionID: migratable.PartitionID,
		TargetNode:  newNode.ID,
	}}
}

// OnNodeLeft fails over the departed node's partitions to the least loaded node.
func (p *RelativePolicy) OnNodeLeft(_ context.Context, node provider.NodeInfo, _ provider.NodeLeaveReason, stats provider.ClusterStats) []provider.BalanceAction {
	var deadPartitions []provider.PartitionInfo
	for _, ns := range stats.Nodes {
		if ns.Node.ID == node.ID {
			deadPartitions = ns.Partitions
			break
		}
	}
	if len(deadPartitions) == 0 {
		return nil
	}

	targetNode := pickLeastLoaded(stats, node.ID)
	if targetNode == "" {
		return nil
	}

	actions := make([]provider.BalanceAction, 0, len(deadPartitions))
	for _, part := range deadPartitions {
		actions = append(actions, provider.BalanceAction{
			Type:        provider.ActionFailover,
			ActorType:   part.ActorType,
			PartitionID: part.PartitionID,
			TargetNode:  targetNode,
		})
	}
	return actions
}
