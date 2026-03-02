package engine

import (
	"context"
	"time"
)

// EvictionScheduler는 idle Actor를 주기적으로 evict한다.
type EvictionScheduler[Req, Resp any] struct {
	host        *ActorHost[Req, Resp]
	idleTimeout time.Duration
	interval    time.Duration
}

// NewEvictionScheduler는 EvictionScheduler를 생성한다.
func NewEvictionScheduler[Req, Resp any](
	host *ActorHost[Req, Resp],
	idleTimeout time.Duration,
	interval time.Duration,
) *EvictionScheduler[Req, Resp] {
	return &EvictionScheduler[Req, Resp]{
		host:        host,
		idleTimeout: idleTimeout,
		interval:    interval,
	}
}

// Start는 eviction 루프를 시작한다. ctx 취소 시 종료.
func (s *EvictionScheduler[Req, Resp]) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			idleSince := time.Now().Add(-s.idleTimeout)
			for _, id := range s.host.IdleActors(idleSince) {
				if err := s.host.Evict(ctx, id); err != nil {
					// eviction 실패는 다음 주기에 재시도
					_ = err
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// CheckpointScheduler는 활성 Actor를 주기적으로 checkpoint한다.
// WAL 누적 기반 자동 checkpoint의 보완 역할.
type CheckpointScheduler[Req, Resp any] struct {
	host     *ActorHost[Req, Resp]
	interval time.Duration
}

// NewCheckpointScheduler는 CheckpointScheduler를 생성한다.
func NewCheckpointScheduler[Req, Resp any](
	host *ActorHost[Req, Resp],
	interval time.Duration,
) *CheckpointScheduler[Req, Resp] {
	return &CheckpointScheduler[Req, Resp]{
		host:     host,
		interval: interval,
	}
}

// Start는 checkpoint 루프를 시작한다. ctx 취소 시 종료.
func (s *CheckpointScheduler[Req, Resp]) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, id := range s.host.ActivePartitions() {
				if err := s.host.Checkpoint(ctx, id); err != nil {
					_ = err
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
