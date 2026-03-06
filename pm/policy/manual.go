package policy

import (
	"context"

	"github.com/oomymy/actorbase/internal/domain"
)

// ManualPolicy는 자동 rebalance를 수행하지 않는다.
// split/migrate는 오직 명시적 RPC(RequestSplit, RequestMigrate)로만 발생한다.
// 노드 장애 발생 시 운영자가 직접 abctl로 처리해야 한다.
type ManualPolicy struct{}

func (p *ManualPolicy) OnNodeJoined(_ context.Context, _ domain.NodeInfo) {}
func (p *ManualPolicy) OnNodeLeft(_ context.Context, _ domain.NodeInfo)   {}
