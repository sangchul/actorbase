package policy

import (
	"context"

	"github.com/sangchul/actorbase/provider"
)

// NoopBalancePolicy는 아무 작업도 수행하지 않는 BalancePolicy 구현체.
// pm.Config.BalancePolicy를 nil로 두면 이 구현체가 사용된다.
type NoopBalancePolicy struct{}

func (p *NoopBalancePolicy) Evaluate(_ context.Context, _ provider.ClusterStats) []provider.BalanceAction {
	return nil
}
func (p *NoopBalancePolicy) OnNodeJoined(_ context.Context, _ provider.NodeInfo, _ provider.ClusterStats) []provider.BalanceAction {
	return nil
}
func (p *NoopBalancePolicy) OnNodeLeft(_ context.Context, _ provider.NodeInfo, _ provider.NodeLeaveReason, _ provider.ClusterStats) []provider.BalanceAction {
	return nil
}
