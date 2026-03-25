package policy

import (
	"context"
	"testing"

	"github.com/sangchul/actorbase/provider"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func makeNodeStats(nodeID string, reachable bool, parts ...provider.PartitionInfo) provider.NodeStats {
	return provider.NodeStats{
		Node:       provider.NodeInfo{ID: nodeID, Addr: nodeID + ":9000"},
		Reachable:  reachable,
		Partitions: parts,
	}
}

func makePartInfo(id, actorType string, keyCount int64, rps float64) provider.PartitionInfo {
	return provider.PartitionInfo{
		PartitionID: id,
		ActorType:   actorType,
		KeyCount:    keyCount,
		RPS:         rps,
	}
}

func makeClusterStats(nodes ...provider.NodeStats) provider.ClusterStats {
	return provider.ClusterStats{Nodes: nodes}
}

var defaultCfg = &ThresholdConfig{
	Split: SplitConfig{
		RPSThreshold: 1000,
		KeyThreshold: 10000,
	},
	Balance: BalanceConfig{
		MaxPartitionDiff: 2,
		RPSImbalancePct:  30.0,
	},
}

// ── Evaluate ─────────────────────────────────────────────────────────────────

func TestThresholdPolicy_Evaluate_SplitByKeyCount(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	stats := makeClusterStats(
		makeNodeStats("n1", true, makePartInfo("p1", "kv", 15000, 100)),
	)

	actions := p.Evaluate(context.Background(), stats)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Type != provider.ActionSplit {
		t.Errorf("expected ActionSplit, got %v", actions[0].Type)
	}
	if actions[0].PartitionID != "p1" {
		t.Errorf("expected partition p1, got %s", actions[0].PartitionID)
	}
}

func TestThresholdPolicy_Evaluate_SplitByRPS(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	stats := makeClusterStats(
		makeNodeStats("n1", true, makePartInfo("p1", "kv", 100, 1500)),
	)

	actions := p.Evaluate(context.Background(), stats)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Type != provider.ActionSplit {
		t.Errorf("expected ActionSplit, got %v", actions[0].Type)
	}
}

func TestThresholdPolicy_Evaluate_NoSplitBelowThreshold(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	stats := makeClusterStats(
		makeNodeStats("n1", true, makePartInfo("p1", "kv", 500, 100)),
	)

	actions := p.Evaluate(context.Background(), stats)
	// no split → only one node so no migrate either
	for _, a := range actions {
		if a.Type == provider.ActionSplit {
			t.Errorf("unexpected ActionSplit: %+v", a)
		}
	}
}

func TestThresholdPolicy_Evaluate_NoSplitForNonCountable(t *testing.T) {
	// KeyCount=-1 means the key_threshold comparison is skipped
	p := NewThresholdPolicy(defaultCfg)
	stats := makeClusterStats(
		makeNodeStats("n1", true, makePartInfo("p1", "kv", -1, 100)),
	)

	actions := p.Evaluate(context.Background(), stats)
	for _, a := range actions {
		if a.Type == provider.ActionSplit {
			t.Error("KeyCount=-1 should not trigger split by key threshold")
		}
	}
}

func TestThresholdPolicy_Evaluate_Migrate(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	// n1: 3 partitions, n2: 0 partitions — diff=3 > MaxPartitionDiff=2
	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfo("p1", "kv", 100, 50),
			makePartInfo("p2", "kv", 100, 50),
			makePartInfo("p3", "kv", 100, 50),
		),
		makeNodeStats("n2", true),
	)

	actions := p.Evaluate(context.Background(), stats)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Type != provider.ActionMigrate {
		t.Errorf("expected ActionMigrate, got %v", actions[0].Type)
	}
	if actions[0].TargetNode != "n2" {
		t.Errorf("expected target n2, got %s", actions[0].TargetNode)
	}
}

func TestThresholdPolicy_Evaluate_NilWhenBalanced(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	// n1: 2 partitions, n2: 1 partition — diff=1 ≤ MaxPartitionDiff=2
	// RPS is also balanced to avoid triggering rpsImbalance
	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfo("p1", "kv", 100, 50),
			makePartInfo("p2", "kv", 100, 50),
		),
		makeNodeStats("n2", true,
			makePartInfo("p3", "kv", 100, 100), // totalRPS = 100, n1 totalRPS = 100 → pct=0%
		),
	)

	actions := p.Evaluate(context.Background(), stats)
	if len(actions) != 0 {
		t.Errorf("expected no actions when balanced, got %d: %+v", len(actions), actions)
	}
}

func TestThresholdPolicy_Evaluate_SkipsUnreachableNode(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	// partitions on an unreachable node are excluded from split candidates

	stats := makeClusterStats(
		makeNodeStats("n1", false, makePartInfo("p1", "kv", 99999, 99999)),
	)

	actions := p.Evaluate(context.Background(), stats)
	for _, a := range actions {
		if a.Type == provider.ActionSplit {
			t.Error("unreachable node should not trigger split")
		}
	}
}

// ── OnNodeLeft ────────────────────────────────────────────────────────────────

func TestThresholdPolicy_OnNodeLeft_Failover(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	deadNode := provider.NodeInfo{ID: "n1", Addr: "n1:9000"}
	stats := makeClusterStats(
		makeNodeStats("n1", false,
			makePartInfo("p1", "kv", -1, 0),
			makePartInfo("p2", "kv", -1, 0),
		),
		makeNodeStats("n2", true),
	)

	actions := p.OnNodeLeft(context.Background(), deadNode, provider.NodeLeaveFailure, stats)
	if len(actions) != 2 {
		t.Fatalf("expected 2 failover actions, got %d", len(actions))
	}
	for _, a := range actions {
		if a.Type != provider.ActionFailover {
			t.Errorf("expected ActionFailover, got %v", a.Type)
		}
		if a.TargetNode != "n2" {
			t.Errorf("expected target n2, got %s", a.TargetNode)
		}
	}
}

func TestThresholdPolicy_OnNodeLeft_NoLiveNodes(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	deadNode := provider.NodeInfo{ID: "n1", Addr: "n1:9000"}
	// only n1 exists and it is dead — no live target nodes
	stats := makeClusterStats(
		makeNodeStats("n1", false, makePartInfo("p1", "kv", -1, 0)),
	)

	actions := p.OnNodeLeft(context.Background(), deadNode, provider.NodeLeaveFailure, stats)
	if len(actions) != 0 {
		t.Errorf("expected no actions when no live nodes, got %d", len(actions))
	}
}

func TestThresholdPolicy_OnNodeLeft_GracefulNoop(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	// Graceful shutdown: dead node has no partitions (drain completed)
	node := provider.NodeInfo{ID: "n1", Addr: "n1:9000"}
	stats := makeClusterStats(
		makeNodeStats("n1", false), // no partitions
		makeNodeStats("n2", true, makePartInfo("p1", "kv", -1, 0)),
	)

	actions := p.OnNodeLeft(context.Background(), node, provider.NodeLeaveGraceful, stats)
	if len(actions) != 0 {
		t.Errorf("graceful leave with no partitions should produce no actions, got %d", len(actions))
	}
}

// ── OnNodeJoined ─────────────────────────────────────────────────────────────

func TestThresholdPolicy_OnNodeJoined_MigratesFromHeaviest(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	newNode := provider.NodeInfo{ID: "n3", Addr: "n3:9000"}
	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfo("p1", "kv", -1, 0),
			makePartInfo("p2", "kv", -1, 0),
		),
		makeNodeStats("n2", true,
			makePartInfo("p3", "kv", -1, 0),
		),
		makeNodeStats("n3", true), // new node (empty)
	)

	actions := p.OnNodeJoined(context.Background(), newNode, stats)
	if len(actions) != 1 {
		t.Fatalf("expected 1 migrate action, got %d", len(actions))
	}
	if actions[0].Type != provider.ActionMigrate {
		t.Errorf("expected ActionMigrate, got %v", actions[0].Type)
	}
	if actions[0].TargetNode != "n3" {
		t.Errorf("expected target n3, got %s", actions[0].TargetNode)
	}
}

func TestThresholdPolicy_OnNodeJoined_NoMigrateIfSinglePartition(t *testing.T) {
	p := NewThresholdPolicy(defaultCfg)
	newNode := provider.NodeInfo{ID: "n2", Addr: "n2:9000"}
	stats := makeClusterStats(
		makeNodeStats("n1", true, makePartInfo("p1", "kv", -1, 0)), // maxCount=1 ≤ 1
		makeNodeStats("n2", true), // new node
	)

	actions := p.OnNodeJoined(context.Background(), newNode, stats)
	if len(actions) != 0 {
		t.Errorf("should not migrate when source has only 1 partition, got %d actions", len(actions))
	}
}

// ── Merge ─────────────────────────────────────────────────────────────────────

func makePartInfoWithRange(id, actorType string, keyCount int64, rps float64, start, end string) provider.PartitionInfo {
	return provider.PartitionInfo{
		PartitionID:   id,
		ActorType:     actorType,
		KeyCount:      keyCount,
		RPS:           rps,
		KeyRangeStart: start,
		KeyRangeEnd:   end,
	}
}

var mergeCfg = &ThresholdConfig{
	Split: SplitConfig{
		RPSThreshold: 1000,
		KeyThreshold: 10000,
	},
	Merge: MergeConfig{
		RPSThreshold: 100,
		KeyThreshold: 1000,
		StableRounds: 3,
	},
	Balance: BalanceConfig{
		MaxPartitionDiff: 2,
		RPSImbalancePct:  30.0,
	},
}

func TestThresholdPolicy_Evaluate_MergeAfterStableRounds(t *testing.T) {
	p := NewThresholdPolicy(mergeCfg)
	ctx := context.Background()

	// adjacent partition pair: p1 [a, m), p2 [m, "")
	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfoWithRange("p1", "kv", 100, 10, "a", "m"),
			makePartInfoWithRange("p2", "kv", 100, 10, "m", ""),
		),
	)

	// rounds 1 and 2: stable_rounds=3 not yet reached → no merge
	for i := 0; i < 2; i++ {
		actions := p.Evaluate(ctx, stats)
		if len(actions) != 0 {
			t.Fatalf("round %d: expected no merge action (stable_rounds not reached), got %d", i+1, len(actions))
		}
	}

	// round 3: stable_rounds reached → merge returned
	actions := p.Evaluate(ctx, stats)
	if len(actions) != 1 {
		t.Fatalf("round 3: expected 1 merge action, got %d", len(actions))
	}
	if actions[0].Type != provider.ActionMerge {
		t.Errorf("expected ActionMerge, got %v", actions[0].Type)
	}
	if actions[0].PartitionID != "p1" {
		t.Errorf("expected lower=p1, got %s", actions[0].PartitionID)
	}
	if actions[0].MergeTarget != "p2" {
		t.Errorf("expected upper=p2, got %s", actions[0].MergeTarget)
	}
}

func TestThresholdPolicy_Evaluate_MergeStableCountResets(t *testing.T) {
	p := NewThresholdPolicy(mergeCfg)
	ctx := context.Background()

	lowStats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfoWithRange("p1", "kv", 100, 10, "a", "m"),
			makePartInfoWithRange("p2", "kv", 100, 10, "m", ""),
		),
	)
	highStats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfoWithRange("p1", "kv", 100, 10, "a", "m"),
			makePartInfoWithRange("p2", "kv", 100, 200, "m", ""), // combined 210 > 100
		),
	)

	// 2 rounds meeting the condition
	p.Evaluate(ctx, lowStats)
	p.Evaluate(ctx, lowStats)
	// 1 round not meeting the condition → reset
	p.Evaluate(ctx, highStats)
	// 2 more rounds meeting the condition → not yet 3 consecutive
	p.Evaluate(ctx, lowStats)
	p.Evaluate(ctx, lowStats)
	actions := p.Evaluate(ctx, lowStats) // now 3 consecutive rounds

	if len(actions) != 1 || actions[0].Type != provider.ActionMerge {
		t.Fatalf("expected merge after reset + 3 stable rounds, got %+v", actions)
	}
}

func TestThresholdPolicy_Evaluate_NoMergeAboveThreshold(t *testing.T) {
	p := NewThresholdPolicy(mergeCfg)
	ctx := context.Background()

	// combined RPS=120 > threshold=100 → no merge
	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfoWithRange("p1", "kv", 100, 60, "a", "m"),
			makePartInfoWithRange("p2", "kv", 100, 60, "m", ""),
		),
	)

	for i := 0; i < 5; i++ {
		actions := p.Evaluate(ctx, stats)
		for _, a := range actions {
			if a.Type == provider.ActionMerge {
				t.Fatalf("round %d: unexpected merge when combined RPS > threshold", i)
			}
		}
	}
}

func TestThresholdPolicy_Evaluate_NoMergeNonAdjacent(t *testing.T) {
	p := NewThresholdPolicy(mergeCfg)
	ctx := context.Background()

	// non-adjacent partitions: p1 [a, g), p2 [m, "") — gap exists
	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfoWithRange("p1", "kv", 100, 10, "a", "g"),
			makePartInfoWithRange("p2", "kv", 100, 10, "m", ""),
		),
	)

	for i := 0; i < 5; i++ {
		actions := p.Evaluate(ctx, stats)
		for _, a := range actions {
			if a.Type == provider.ActionMerge {
				t.Fatalf("round %d: unexpected merge for non-adjacent partitions", i)
			}
		}
	}
}

func TestThresholdPolicy_Evaluate_NoMergeConfigDisabled(t *testing.T) {
	// no merge config means merge is disabled
	p := NewThresholdPolicy(defaultCfg)
	ctx := context.Background()

	stats := makeClusterStats(
		makeNodeStats("n1", true,
			makePartInfoWithRange("p1", "kv", 10, 1, "a", "m"),
			makePartInfoWithRange("p2", "kv", 10, 1, "m", ""),
		),
	)

	for i := 0; i < 5; i++ {
		actions := p.Evaluate(ctx, stats)
		for _, a := range actions {
			if a.Type == provider.ActionMerge {
				t.Fatalf("round %d: unexpected merge when merge config is disabled", i)
			}
		}
	}
}
