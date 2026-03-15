package policy

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sangchul/actorbase/provider"
)

// ── 공통 설정 타입 ─────────────────────────────────────────────────────────────

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

// BalanceConfig는 노드 간 파티션 이동 기준. 여러 알고리즘이 공유한다.
type BalanceConfig struct {
	MaxPartitionDiff int     `yaml:"max_partition_diff"` // 노드 간 파티션 수 허용 차이
	RPSImbalancePct  float64 `yaml:"rps_imbalance_pct"`  // 노드 간 RPS 차이 허용 %
}

// ── ParsePolicy ────────────────────────────────────────────────────────────────

// ParsePolicy는 YAML 바이트를 파싱해 BalancePolicy와 RunnerConfig를 반환한다.
// algorithm 필드 값으로 구현체를 선택한다. 새 알고리즘은 이 함수에만 case를 추가하면 된다.
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

// ── 공유 헬퍼 ─────────────────────────────────────────────────────────────────

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
