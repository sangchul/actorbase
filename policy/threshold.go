package policy

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sangchul/actorbase/provider"
)

// RunnerConfig는 balancerRunner 실행에 필요한 파라미터.
// 알고리즘에 독립적인 실행 주기와 cooldown 설정을 담는다.
type RunnerConfig struct {
	CheckInterval time.Duration
	Cooldown      CooldownConfig
}

// CooldownConfig는 split/migrate 후 대기 시간 설정.
type CooldownConfig struct {
	Global    time.Duration `yaml:"global"`    // 전체 클러스터 대기
	Partition time.Duration `yaml:"partition"` // 파티션별 대기
}

// ThresholdConfig는 threshold 알고리즘 기반 자동 split/balance 정책 설정.
type ThresholdConfig struct {
	Algorithm     string         `yaml:"algorithm"`      // "threshold"
	CheckInterval time.Duration  `yaml:"check_interval"` // ex. 30s
	Cooldown      CooldownConfig `yaml:"cooldown"`
	Split         SplitConfig    `yaml:"split"`
	Balance       BalanceConfig  `yaml:"balance"`
}

// SplitConfig는 자동 split 임계값 설정.
type SplitConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"` // 파티션 RPS가 이 값을 초과하면 split
	KeyThreshold int64   `yaml:"key_threshold"` // 파티션 key 수가 이 값을 초과하면 split
}

// BalanceConfig는 자동 balance(migrate) 임계값 설정.
type BalanceConfig struct {
	MaxPartitionDiff int     `yaml:"max_partition_diff"` // 노드 간 파티션 수 허용 차이
	RPSImbalancePct  float64 `yaml:"rps_imbalance_pct"`  // 노드 간 RPS 차이 허용 %
}

// ParsePolicy는 YAML 바이트를 파싱해 BalancePolicy와 RunnerConfig를 반환한다.
// algorithm 필드 값으로 구현체를 선택한다. 새 알고리즘은 이 함수에만 case를 추가하면 된다.
func ParsePolicy(data []byte) (provider.BalancePolicy, *RunnerConfig, error) {
	// algorithm 및 공통 실행 파라미터 먼저 파싱
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
	default:
		return nil, nil, fmt.Errorf("policy: unsupported algorithm %q (supported: threshold)", header.Algorithm)
	}
}

// ThresholdPolicy는 ThresholdConfig 기반 provider.BalancePolicy 구현체.
// YAML로 설정 가능한 기본 제공 분배 정책이다.
type ThresholdPolicy struct {
	cfg *ThresholdConfig
}

// NewThresholdPolicy는 ThresholdConfig로부터 ThresholdPolicy를 생성한다.
func NewThresholdPolicy(cfg *ThresholdConfig) *ThresholdPolicy {
	return &ThresholdPolicy{cfg: cfg}
}

// Evaluate는 stats 기반으로 split/migrate 액션을 반환한다.
// split은 한 사이클에 하나만 반환한다 (연쇄 split 방지).
func (p *ThresholdPolicy) Evaluate(_ context.Context, stats provider.ClusterStats) []provider.BalanceAction {
	var actions []provider.BalanceAction

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
			actions = append(actions, provider.BalanceAction{
				Type:        provider.ActionSplit,
				ActorType:   part.ActorType,
				PartitionID: part.PartitionID,
				SplitKey:    splitKey,
			})
			return actions // split은 하나만
		}
	}

	// balance 대상 탐색 (파티션 수 기준)
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
		for _, p := range ns.Partitions {
			rps += p.RPS
		}
		summaries = append(summaries, summary{
			nodeID:         ns.Node.ID,
			partitionCount: len(ns.Partitions),
			totalRPS:       rps,
		})
	}
	if len(summaries) < 2 {
		return actions
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
		return actions
	}

	// maxNode에서 파티션 하나 선택
	for _, ns := range stats.Nodes {
		if !ns.Reachable || ns.Node.ID != maxNode.nodeID {
			continue
		}
		if len(ns.Partitions) == 0 {
			break
		}
		part := ns.Partitions[0]
		actions = append(actions, provider.BalanceAction{
			Type:        provider.ActionMigrate,
			ActorType:   part.ActorType,
			PartitionID: part.PartitionID,
			TargetNode:  minNode.nodeID,
		})
		break
	}
	return actions
}

// OnNodeJoined는 새 노드 합류 시 부하가 높은 노드에서 migrate한다.
func (p *ThresholdPolicy) OnNodeJoined(_ context.Context, newNode provider.NodeInfo, stats provider.ClusterStats) []provider.BalanceAction {
	// 새 노드는 파티션이 없으므로 가장 많은 노드에서 하나 이전
	var maxCount int
	var maxNodeID string
	var migratable *provider.PartitionInfo

	for _, ns := range stats.Nodes {
		if !ns.Reachable || ns.Node.ID == newNode.ID {
			continue
		}
		if len(ns.Partitions) > maxCount {
			maxCount = len(ns.Partitions)
			maxNodeID = ns.Node.ID
			p := ns.Partitions[0]
			migratable = &p
		}
	}
	_ = maxNodeID
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

// OnNodeLeft는 노드 이탈 시 해당 노드의 파티션을 처리한다.
// Failure: dead node의 파티션에 ActionFailover 반환.
// Graceful: drainPartitions가 이미 이전 완료했으므로 남은 파티션만 Failover.
func (p *ThresholdPolicy) OnNodeLeft(_ context.Context, node provider.NodeInfo, _ provider.NodeLeaveReason, stats provider.ClusterStats) []provider.BalanceAction {
	// dead node의 파티션 목록 (Reachable=false, 라우팅 테이블 기반)
	var deadPartitions []provider.PartitionInfo
	for _, ns := range stats.Nodes {
		if ns.Node.ID == node.ID {
			deadPartitions = ns.Partitions
			break
		}
	}
	if len(deadPartitions) == 0 {
		return nil // 파티션 없음 (graceful drain 완료 등)
	}

	// live 노드 중 파티션 가장 적은 노드 선택
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

// pickLeastLoaded는 excludeNodeID를 제외한 Reachable 노드 중 파티션이 가장 적은 노드 ID를 반환한다.
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

// keyRangeMidpoint는 [start, end) 키 범위의 중간값을 계산한다.
func keyRangeMidpoint(start, end string) string {
	if start == "" && end == "" {
		return "m"
	}
	if end == "" {
		return start + "m"
	}
	sb := []byte(start)
	eb := []byte(end)
	maxLen := len(eb)
	if len(sb) > maxLen {
		maxLen = len(sb)
	}
	for len(sb) < maxLen {
		sb = append(sb, 0)
	}
	for len(eb) < maxLen {
		eb = append(eb, 0)
	}
	result := make([]byte, maxLen)
	carry := 0
	for i := maxLen - 1; i >= 0; i-- {
		sum := int(sb[i]) + int(eb[i]) + carry*256
		result[i] = byte(sum / 2)
		carry = sum % 2
	}
	n := len(result)
	for n > 0 && result[n-1] == 0 {
		n--
	}
	if n == 0 {
		n = 1
	}
	mid := string(result[:n])
	if mid <= start {
		return start + "m"
	}
	return mid
}
