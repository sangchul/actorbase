package provider

import "context"

// ── 타입 정의 ──────────────────────────────────────────────────────────────────

// NodeStatus는 노드의 상태.
type NodeStatus int

const (
	NodeStatusActive   NodeStatus = iota
	NodeStatusDraining            // 파티션 이전 중 (graceful shutdown)
)

// NodeInfo는 클러스터 노드 정보.
type NodeInfo struct {
	ID     string
	Addr   string
	Status NodeStatus
}

// NodeLeaveReason은 노드 이탈 원인.
type NodeLeaveReason int

const (
	// NodeLeaveGraceful은 정상 종료. drainPartitions가 파티션 이전을 완료한 상태.
	NodeLeaveGraceful NodeLeaveReason = iota
	// NodeLeaveFailure는 장애(SIGKILL, 네트워크 단절 등). etcd lease 만료로 감지.
	NodeLeaveFailure
)

// PartitionInfo는 파티션 하나의 상태와 통계.
// GetStats(RPS, KeyCount)와 라우팅 테이블(KeyRange, NodeID)을 합산하여 구성된다.
type PartitionInfo struct {
	PartitionID   string
	ActorType     string
	KeyCount      int64   // -1: actor가 Countable 미구현
	RPS           float64 // 60초 슬라이딩 윈도우 평균
	KeyRangeStart string
	KeyRangeEnd   string
}

// NodeStats는 노드 하나의 상태.
type NodeStats struct {
	Node       NodeInfo
	Reachable  bool            // false면 GetStats 실패. Partitions는 라우팅 테이블 기반.
	NodeRPS    float64         // Reachable=false 이면 0
	Partitions []PartitionInfo
}

// ClusterStats는 BalancePolicy 메서드 호출 시 전달되는 클러스터 상태 스냅샷.
type ClusterStats struct {
	Nodes []NodeStats
}

// ── BalanceAction ──────────────────────────────────────────────────────────────

// BalanceActionType은 실행할 rebalance 작업의 종류.
type BalanceActionType int

const (
	// ActionSplit은 파티션을 SplitKey 기준으로 분할한다.
	ActionSplit BalanceActionType = iota
	// ActionMigrate는 살아있는 PS에서 다른 노드로 파티션을 이전한다.
	ActionMigrate
	// ActionFailover는 장애 노드의 파티션을 복구한다.
	// source PS가 죽었으므로 PreparePartition만 수행한다 (ExecuteMigrateOut 생략).
	ActionFailover
)

// BalanceAction은 PM이 실행할 rebalance 작업 하나.
type BalanceAction struct {
	Type        BalanceActionType
	ActorType   string
	PartitionID string
	SplitKey    string // ActionSplit 전용
	TargetNode  string // ActionMigrate / ActionFailover 전용
}

// ── BalancePolicy 인터페이스 ───────────────────────────────────────────────────

// BalancePolicy는 PM의 분배 전략을 정의하는 인터페이스.
//
// 사용자가 직접 구현하거나, pm/policy의 내장 구현체(ThresholdPolicy)를 YAML로 로드한다.
// pm.Config.BalancePolicy에 주입하면 PM이 사용한다.
//
// 반환된 []BalanceAction은 PM이 순서대로 실행한다.
// nil 또는 빈 슬라이스를 반환하면 아무 작업도 수행하지 않는다.
type BalancePolicy interface {
	// Evaluate는 check_interval마다 호출된다.
	// 현재 클러스터 stats를 기반으로 split/migrate 액션을 반환한다.
	Evaluate(ctx context.Context, stats ClusterStats) []BalanceAction

	// OnNodeJoined는 새 노드가 클러스터에 합류했을 때 호출된다.
	// 부하 재분배를 위한 migrate 액션을 반환할 수 있다.
	OnNodeJoined(ctx context.Context, node NodeInfo, stats ClusterStats) []BalanceAction

	// OnNodeLeft는 노드가 클러스터를 이탈했을 때 호출된다.
	// reason으로 정상 종료(Graceful)와 장애(Failure)를 구분한다.
	//
	// Graceful: drainPartitions가 이미 파티션을 이전 완료했으므로 보통 no-op.
	//
	// Failure: source PS가 죽었으므로 ActionFailover를 반환해야 파티션이 복구된다.
	//   stats에 dead node의 파티션 목록(라우팅 테이블 기반, Reachable=false)이 포함된다.
	OnNodeLeft(ctx context.Context, node NodeInfo, reason NodeLeaveReason, stats ClusterStats) []BalanceAction
}
