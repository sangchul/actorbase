package policy

import (
	"context"

	"github.com/sangchul/actorbase/provider"
)

// RelativeConfig는 relative 알고리즘 기반 자동 split/balance 정책 설정 (YAML 파싱용).
//
// 예시 YAML:
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

// RelativeSplit은 relative 알고리즘의 split 설정.
type RelativeSplit struct {
	RPSMultiplier float64 `yaml:"rps_multiplier"` // 평균 RPS 대비 배수 (예: 3.0)
	MinAvgRPS     float64 `yaml:"min_avg_rps"`    // split 고려 최소 평균 RPS
}

// NewRelativePolicy는 RelativeConfig로부터 RelativePolicy를 생성한다.
func NewRelativePolicy(cfg *RelativeConfig) *RelativePolicy {
	return &RelativePolicy{
		SplitRPSMultiplier: cfg.Split.RPSMultiplier,
		MinAvgRPS:          cfg.Split.MinAvgRPS,
		Balance:            cfg.Balance,
	}
}

// RelativePolicy는 클러스터 평균 대비 상대적 기준으로 split/migrate를 판단하는 BalancePolicy 구현체.
//
// ThresholdPolicy가 절대 임계값(rps > 1000)을 사용하는 것과 달리,
// RelativePolicy는 클러스터 전체 파티션의 평균 RPS 대비 배수로 split을 판단한다.
//
// 예) SplitRPSMultiplier=3.0, 평균 RPS=200 → 600 초과 파티션만 split.
// 트래픽이 낮을 땐 자동으로 기준이 낮아지고, 높을 땐 올라간다.
// MinAvgRPS로 트래픽이 너무 낮을 때 불필요한 split을 방지한다.
type RelativePolicy struct {
	// SplitRPSMultiplier는 split 기준 배수.
	// 파티션 RPS가 (클러스터 평균 파티션 RPS × 배수)를 초과하면 split한다.
	// 예) 3.0 → 평균의 3배 초과 시 split.
	SplitRPSMultiplier float64

	// MinAvgRPS는 split을 고려하기 위한 최소 평균 RPS.
	// 클러스터 평균 파티션 RPS가 이 값 미만이면 split하지 않는다.
	// 트래픽이 극히 낮을 때 노이즈로 인한 split을 방지한다.
	MinAvgRPS float64

	// Balance는 노드 간 파티션 이동 기준.
	// ThresholdPolicy와 동일한 설정 구조를 사용한다.
	Balance BalanceConfig
}

// Evaluate는 클러스터 평균 대비 상대적 기준으로 split/migrate 액션을 반환한다.
func (p *RelativePolicy) Evaluate(_ context.Context, stats provider.ClusterStats) []provider.BalanceAction {
	// ── split 판단 ──────────────────────────────────────────────────────────────

	// 전체 파티션의 평균 RPS 계산
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
		}
	}

	// ── balance 판단 (ThresholdPolicy와 동일) ───────────────────────────────────

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

// OnNodeJoined는 새 노드 합류 시 파티션이 가장 많은 노드에서 하나를 이전한다.
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

// OnNodeLeft는 노드 이탈 시 dead node의 파티션을 가장 여유로운 노드로 failover한다.
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
