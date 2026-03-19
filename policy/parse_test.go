package policy

import (
	"testing"
	"time"
)

func TestParsePolicy_Threshold(t *testing.T) {
	data := []byte(`
algorithm: threshold
check_interval: 30s
cooldown:
  global: 60s
  partition: 120s
split:
  rps_threshold: 1000.0
  key_threshold: 10000
balance:
  max_partition_diff: 2
  rps_imbalance_pct: 30.0
`)
	pol, runCfg, err := ParsePolicy(data)
	if err != nil {
		t.Fatalf("ParsePolicy(threshold): %v", err)
	}
	if _, ok := pol.(*ThresholdPolicy); !ok {
		t.Errorf("expected *ThresholdPolicy, got %T", pol)
	}
	if runCfg.CheckInterval != 30*time.Second {
		t.Errorf("CheckInterval = %v, want 30s", runCfg.CheckInterval)
	}
	if runCfg.Cooldown.Global != 60*time.Second {
		t.Errorf("Cooldown.Global = %v, want 60s", runCfg.Cooldown.Global)
	}
	if runCfg.Cooldown.Partition != 120*time.Second {
		t.Errorf("Cooldown.Partition = %v, want 120s", runCfg.Cooldown.Partition)
	}
}

func TestParsePolicy_Relative(t *testing.T) {
	data := []byte(`
algorithm: relative
check_interval: 15s
cooldown:
  global: 30s
  partition: 60s
split:
  rps_multiplier: 3.0
  min_avg_rps: 10.0
balance:
  max_partition_diff: 2
  rps_imbalance_pct: 25.0
`)
	pol, runCfg, err := ParsePolicy(data)
	if err != nil {
		t.Fatalf("ParsePolicy(relative): %v", err)
	}
	if _, ok := pol.(*RelativePolicy); !ok {
		t.Errorf("expected *RelativePolicy, got %T", pol)
	}
	if runCfg.CheckInterval != 15*time.Second {
		t.Errorf("CheckInterval = %v, want 15s", runCfg.CheckInterval)
	}
}

func TestParsePolicy_InvalidAlgorithm(t *testing.T) {
	data := []byte("algorithm: unknown\ncheck_interval: 30s\n")
	_, _, err := ParsePolicy(data)
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
}

func TestParsePolicy_MissingCheckInterval(t *testing.T) {
	data := []byte("algorithm: threshold\n")
	_, _, err := ParsePolicy(data)
	if err == nil {
		t.Fatal("expected error for missing/zero check_interval")
	}
}

func TestParsePolicy_InvalidYAML(t *testing.T) {
	data := []byte(":\t: invalid yaml {{{{")
	_, _, err := ParsePolicy(data)
	if err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}
