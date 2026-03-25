package policy

import (
	"context"
	"sort"
	"sync"

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
	Merge         MergeConfig   `yaml:"merge"`
	Balance       BalanceConfig `yaml:"balance"`
}

// MergeConfig는 threshold 알고리즘의 merge 임계값 설정.
type MergeConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"`  // 두 파티션 합산 RPS가 이 값 미만이면 merge 후보
	KeyThreshold int64   `yaml:"key_threshold"`  // 두 파티션 합산 key count가 이 값 미만이면 merge 후보
	StableRounds int     `yaml:"stable_rounds"`  // N회 연속 조건 충족 시 실행 (진동 방지)
}

// SplitConfig는 threshold 알고리즘의 split 임계값 설정.
type SplitConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"` // 파티션 RPS가 이 값을 초과하면 split
	KeyThreshold int64   `yaml:"key_threshold"` // 파티션 key 수가 이 값을 초과하면 split
}

// ThresholdPolicy는 절대 임계값 기반 provider.BalancePolicy 구현체.
type ThresholdPolicy struct {
	cfg *ThresholdConfig

	mu                sync.Mutex
	mergeStableCount  map[string]int // key: "lowerID:upperID" → 연속 충족 횟수
}

// NewThresholdPolicy는 ThresholdConfig로부터 ThresholdPolicy를 생성한다.
func NewThresholdPolicy(cfg *ThresholdConfig) *ThresholdPolicy {
	if cfg.Merge.StableRounds <= 0 {
		cfg.Merge.StableRounds = 3
	}
	return &ThresholdPolicy{
		cfg:              cfg,
		mergeStableCount: make(map[string]int),
	}
}

// Evaluate는 절대 임계값 기반으로 split/migrate 액션을 반환한다.
// split은 한 사이클에 하나만 반환한다 (연쇄 split 방지).
func (p *ThresholdPolicy) Evaluate(_ context.Context, stats provider.ClusterStats) []provider.BalanceAction {
	// split 대상 탐색 (첫 번째 대상만). split key는 PS가 결정한다 (SplitHinter 또는 midpoint).
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

	// merge 대상 탐색 (merge config가 설정된 경우에만)
	if action := p.evaluateMerge(stats); action != nil {
		return []provider.BalanceAction{*action}
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

// evaluateMerge는 인접 파티션 쌍에 대해 merge 조건을 평가한다.
// stable_rounds 연속 충족 시 ActionMerge를 반환한다.
// 한 사이클에 merge 하나만 반환한다 (연쇄 merge 방지).
func (p *ThresholdPolicy) evaluateMerge(stats provider.ClusterStats) *provider.BalanceAction {
	mergeCfg := p.cfg.Merge
	if mergeCfg.RPSThreshold <= 0 && mergeCfg.KeyThreshold <= 0 {
		return nil // merge 설정 없음
	}

	// actorType별로 파티션을 수집
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

	// 이번 사이클의 merge 후보 key 집합 (stable count 정리용)
	currentCandidates := make(map[string]bool)

	p.mu.Lock()
	defer p.mu.Unlock()

	for actorType, parts := range byType {
		_ = actorType
		// KeyRangeStart 기준 정렬
		sort.Slice(parts, func(i, j int) bool {
			return parts[i].KeyRangeStart < parts[j].KeyRangeStart
		})

		// 인접 쌍 순회
		for i := 0; i < len(parts)-1; i++ {
			lower := parts[i]
			upper := parts[i+1]

			// 인접 확인: lower.End == upper.Start
			if lower.KeyRangeEnd != upper.KeyRangeStart {
				continue
			}

			// RPS 조건 확인
			combinedRPS := lower.RPS + upper.RPS
			rpsOK := mergeCfg.RPSThreshold <= 0 || combinedRPS < mergeCfg.RPSThreshold

			// key count 조건 확인 (-1이면 미구현, skip)
			keyOK := mergeCfg.KeyThreshold <= 0
			if !keyOK && lower.KeyCount >= 0 && upper.KeyCount >= 0 {
				keyOK = lower.KeyCount+upper.KeyCount < mergeCfg.KeyThreshold
			} else if lower.KeyCount < 0 || upper.KeyCount < 0 {
				keyOK = true // Countable 미구현 시 key count 조건 skip
			}

			if !rpsOK || !keyOK {
				continue
			}

			candidateKey := lower.PartitionID + ":" + upper.PartitionID
			currentCandidates[candidateKey] = true
			p.mergeStableCount[candidateKey]++

			if p.mergeStableCount[candidateKey] >= mergeCfg.StableRounds {
				// stable_rounds 달성 — merge 실행
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

	// 이번 사이클에 후보가 아닌 항목의 stable count 리셋
	for key := range p.mergeStableCount {
		if !currentCandidates[key] {
			delete(p.mergeStableCount, key)
		}
	}

	return nil
}
