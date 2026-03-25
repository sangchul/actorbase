package policy

import (
	"context"
	"sort"
	"sync"

	"github.com/sangchul/actorbase/provider"
)

// ThresholdConfig holds the configuration for the threshold algorithm-based automatic split/balance policy (for YAML parsing).
//
// Example YAML:
//
//	algorithm: threshold
//	check_interval: 30s
//	cooldown:
//	  global: 60s
//	  partition: 120s
//	split:
//	  rps_threshold: 1000
//	  key_threshold: 10000
//	balance:
//	  max_partition_diff: 2
//	  rps_imbalance_pct: 30
type ThresholdConfig struct {
	Algorithm     string        `yaml:"algorithm"`
	CheckInterval interface{}   `yaml:"-"` // handled by RunnerConfig
	Cooldown      interface{}   `yaml:"-"` // handled by RunnerConfig
	Split         SplitConfig   `yaml:"split"`
	Merge         MergeConfig   `yaml:"merge"`
	Balance       BalanceConfig `yaml:"balance"`
}

// MergeConfig holds the merge threshold settings for the threshold algorithm.
type MergeConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"`  // merge candidate when combined RPS of two partitions is below this value
	KeyThreshold int64   `yaml:"key_threshold"`  // merge candidate when combined key count of two partitions is below this value
	StableRounds int     `yaml:"stable_rounds"`  // execute after N consecutive rounds meeting the condition (prevents oscillation)
}

// SplitConfig holds the split threshold settings for the threshold algorithm.
type SplitConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"` // split a partition when its RPS exceeds this value
	KeyThreshold int64   `yaml:"key_threshold"` // split a partition when its key count exceeds this value
}

// ThresholdPolicy is a provider.BalancePolicy implementation based on absolute thresholds.
type ThresholdPolicy struct {
	cfg *ThresholdConfig

	mu               sync.Mutex
	mergeStableCount map[string]int // key: "lowerID:upperID" → consecutive rounds meeting the condition
}

// NewThresholdPolicy creates a ThresholdPolicy from a ThresholdConfig.
func NewThresholdPolicy(cfg *ThresholdConfig) *ThresholdPolicy {
	if cfg.Merge.StableRounds <= 0 {
		cfg.Merge.StableRounds = 3
	}
	return &ThresholdPolicy{
		cfg:              cfg,
		mergeStableCount: make(map[string]int),
	}
}

// Evaluate returns split/migrate actions based on absolute thresholds.
// Only one split is returned per cycle (prevents cascading splits).
func (p *ThresholdPolicy) Evaluate(_ context.Context, stats provider.ClusterStats) []provider.BalanceAction {
	// search for split candidates (first match only). the split key is determined by the PS (via SplitHinter or midpoint).
	for _, ns := range stats.Nodes {
		if !ns.Reachable {
			continue
		}
		for _, part := range ns.Partitions {
			needSplit := (p.cfg.Split.RPSThreshold > 0 && part.RPS > p.cfg.Split.RPSThreshold) ||
				(p.cfg.Split.KeyThreshold > 0 && part.KeyCount >= 0 && part.KeyCount > p.cfg.Split.KeyThreshold)
			if !needSplit {
				continue
			}
			return []provider.BalanceAction{{
				Type:        provider.ActionSplit,
				ActorType:   part.ActorType,
				PartitionID: part.PartitionID,
			}}
		}
	}

	// search for merge candidates (only when merge config is configured)
	if action := p.evaluateMerge(stats); action != nil {
		return []provider.BalanceAction{*action}
	}

	// search for balance candidates
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
		for _, pt := range ns.Partitions {
			rps += pt.RPS
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
		rpsImbalance = pct > p.cfg.Balance.RPSImbalancePct
	}
	if partDiff <= p.cfg.Balance.MaxPartitionDiff && !rpsImbalance {
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
func (p *ThresholdPolicy) OnNodeJoined(_ context.Context, newNode provider.NodeInfo, stats provider.ClusterStats) []provider.BalanceAction {
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
func (p *ThresholdPolicy) OnNodeLeft(_ context.Context, node provider.NodeInfo, _ provider.NodeLeaveReason, stats provider.ClusterStats) []provider.BalanceAction {
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

// evaluateMerge evaluates merge conditions for adjacent partition pairs.
// Returns ActionMerge when the condition is met for stable_rounds consecutive rounds.
// Only one merge is returned per cycle (prevents cascading merges).
func (p *ThresholdPolicy) evaluateMerge(stats provider.ClusterStats) *provider.BalanceAction {
	mergeCfg := p.cfg.Merge
	if mergeCfg.RPSThreshold <= 0 && mergeCfg.KeyThreshold <= 0 {
		return nil // no merge configuration
	}

	// collect partitions grouped by actorType
	type partInfo struct {
		provider.PartitionInfo
		nodeID string
	}
	byType := make(map[string][]partInfo)
	for _, ns := range stats.Nodes {
		if !ns.Reachable {
			continue
		}
		for _, pt := range ns.Partitions {
			byType[pt.ActorType] = append(byType[pt.ActorType], partInfo{
				PartitionInfo: pt,
				nodeID:        ns.Node.ID,
			})
		}
	}

	// set of merge candidate keys for this cycle (used to clean up stable counts)
	currentCandidates := make(map[string]bool)

	p.mu.Lock()
	defer p.mu.Unlock()

	for actorType, parts := range byType {
		_ = actorType
		// sort by KeyRangeStart
		sort.Slice(parts, func(i, j int) bool {
			return parts[i].KeyRangeStart < parts[j].KeyRangeStart
		})

		// iterate over adjacent pairs
		for i := 0; i < len(parts)-1; i++ {
			lower := parts[i]
			upper := parts[i+1]

			// verify adjacency: lower.End == upper.Start
			if lower.KeyRangeEnd != upper.KeyRangeStart {
				continue
			}

			// check RPS condition
			combinedRPS := lower.RPS + upper.RPS
			rpsOK := mergeCfg.RPSThreshold <= 0 || combinedRPS < mergeCfg.RPSThreshold

			// check key count condition (-1 means not implemented, skip)
			keyOK := mergeCfg.KeyThreshold <= 0
			if !keyOK && lower.KeyCount >= 0 && upper.KeyCount >= 0 {
				keyOK = lower.KeyCount+upper.KeyCount < mergeCfg.KeyThreshold
			} else if lower.KeyCount < 0 || upper.KeyCount < 0 {
				keyOK = true // skip key count condition when Countable is not implemented
			}

			if !rpsOK || !keyOK {
				continue
			}

			candidateKey := lower.PartitionID + ":" + upper.PartitionID
			currentCandidates[candidateKey] = true
			p.mergeStableCount[candidateKey]++

			if p.mergeStableCount[candidateKey] >= mergeCfg.StableRounds {
				// stable_rounds reached — execute merge
				delete(p.mergeStableCount, candidateKey)
				return &provider.BalanceAction{
					Type:        provider.ActionMerge,
					ActorType:   lower.ActorType,
					PartitionID: lower.PartitionID,
					MergeTarget: upper.PartitionID,
				}
			}
		}
	}

	// reset stable count for entries that are no longer candidates this cycle
	for key := range p.mergeStableCount {
		if !currentCandidates[key] {
			delete(p.mergeStableCount, key)
		}
	}

	return nil
}
