package policy

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// ThresholdConfig는 threshold 알고리즘 기반 자동 split/balance 정책 설정.
type ThresholdConfig struct {
	Algorithm     string         `yaml:"algorithm"`      // "threshold"
	CheckInterval time.Duration  `yaml:"check_interval"` // ex. 30s
	Cooldown      CooldownConfig `yaml:"cooldown"`
	Split         SplitConfig    `yaml:"split"`
	Balance       BalanceConfig  `yaml:"balance"`
}

// CooldownConfig는 split/migrate 후 대기 시간 설정.
type CooldownConfig struct {
	Global    time.Duration `yaml:"global"`    // 전체 클러스터 대기
	Partition time.Duration `yaml:"partition"` // 파티션별 대기
}

// SplitConfig는 자동 split 임계값 설정.
type SplitConfig struct {
	RPSThreshold float64 `yaml:"rps_threshold"` // 파티션 RPS가 이 값을 초과하면 split
	KeyThreshold int64   `yaml:"key_threshold"` // 파티션 key 수가 이 값을 초과하면 split
}

// BalanceConfig는 자동 balance(migrate) 임계값 설정.
type BalanceConfig struct {
	MaxPartitionDiff int     `yaml:"max_partition_diff"`  // 노드 간 파티션 수 허용 차이
	RPSImbalancePct  float64 `yaml:"rps_imbalance_pct"`   // 노드 간 RPS 차이 허용 %
}

// ParsePolicy는 YAML 바이트를 ThresholdConfig로 파싱한다.
// algorithm 필드가 "threshold"가 아니면 에러를 반환한다.
func ParsePolicy(data []byte) (*ThresholdConfig, error) {
	var cfg ThresholdConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("policy: parse YAML: %w", err)
	}
	if cfg.Algorithm != "threshold" {
		return nil, fmt.Errorf("policy: unsupported algorithm %q (supported: threshold)", cfg.Algorithm)
	}
	if cfg.CheckInterval <= 0 {
		return nil, fmt.Errorf("policy: check_interval must be positive")
	}
	return &cfg, nil
}
