package policy

import (
	"context"

	"github.com/sangchul/actorbase/provider"
)

// ThresholdConfig는 threshold 알고리즘 기반 자동 split/balance 정책 설정 (YAML 파싱용).
//
// 예시 YAML:
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
	CheckInterval interface{}   `yaml:"-"` // RunnerConfig에서 처리
	Cooldown      interface{}   `yaml:"-"` // RunnerConfig에서 처리
	Split         SplitConfig   `yaml:"split"`
	Balance       BalanceConfig `yaml:"balance"`
}

// SplitConfig는 threshold 알고리즘의 split 임계값 설정.
type SplitConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"` // 파티션 RPS가 이 값을 초과하면 split
	KeyThreshold int64   `yaml:"key_threshold"` // 파티션 key 수가 이 값을 초과하면 split
}

// ThresholdPolicy는 절대 임계값 기반 provider.BalancePolicy 구현체.
type ThresholdPolicy struct {
	cfg *ThresholdConfig
}

// NewThresholdPolicy는 ThresholdConfig로부터 ThresholdPolicy를 생성한다.
func NewThresholdPolicy(cfg *ThresholdConfig) *ThresholdPolicy {
	return &ThresholdPolicy{cfg: cfg}
}

// Evaluate는 절대 임계값 기반으로 split/migrate 액션을 반환한다.
// split은 한 사이클에 하나만 반환한다 (연쇄 split 방지).
func (p *ThresholdPolicy) Evaluate(_ context.Context, stats provider.ClusterStats) []provider.BalanceAction {
	// split 대상 탐색 (첫 번째 대상만)
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
			splitKey := keyRangeMidpoint(part.KeyRangeStart, part.KeyRangeEnd)
			if splitKey == "" {
				continue
			}
			return []provider.BalanceAction{{
				Type:        provider.ActionSplit,
				ActorType:   part.ActorType,
				PartitionID: part.PartitionID,
				SplitKey:    splitKey,
			}}
		}
	}

	// balance 대상 탐색
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

// OnNodeJoined는 새 노드 합류 시 파티션이 가장 많은 노드에서 하나를 이전한다.
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

// OnNodeLeft는 노드 이탈 시 dead node의 파티션을 가장 여유로운 노드로 failover한다.
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
