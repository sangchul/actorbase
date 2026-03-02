package domain

// NodeStatus는 노드의 현재 상태를 나타낸다.
type NodeStatus int

const (
	// NodeStatusActive: 노드가 정상 동작 중이며 요청을 수락한다.
	NodeStatusActive NodeStatus = iota

	// NodeStatusDraining: 파티션 migration 진행 중.
	// 새 요청은 받지만, 곧 파티션을 반납할 예정이다.
	NodeStatusDraining
)

// NodeInfo는 클러스터 내 하나의 노드에 대한 메타데이터.
type NodeInfo struct {
	ID      string     // 클러스터 내 유일한 노드 식별자
	Address string     // gRPC 접속 주소 ("host:port")
	Status  NodeStatus
}
