package policy

import (
	"context"

	"github.com/oomymy/actorbase/internal/domain"
)

// RebalancePolicy는 클러스터 이벤트에 반응하여 rebalance를 수행하는 전략 인터페이스.
// Server가 멤버십 이벤트 발생 시 호출한다.
type RebalancePolicy interface {
	// OnNodeJoined는 새 노드가 클러스터에 합류했을 때 호출된다.
	// 파티션 부하를 균등하게 하기 위해 일부 파티션을 새 노드로 migrate할 수 있다.
	OnNodeJoined(ctx context.Context, node domain.NodeInfo)

	// OnNodeLeft는 노드가 클러스터를 떠났을 때 호출된다.
	// 해당 노드의 파티션을 다른 노드로 재배치해야 한다.
	OnNodeLeft(ctx context.Context, node domain.NodeInfo)
}
