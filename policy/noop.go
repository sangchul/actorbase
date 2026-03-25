package policy

import (
	"context"

	"github.com/sangchul/actorbase/provider"
)

// NoopBalancePolicy is a BalancePolicy implementation that performs no operations.
// This implementation is used when pm.Config.BalancePolicy is left as nil.
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
