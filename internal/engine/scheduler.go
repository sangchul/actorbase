package engine

import (
	"context"
	"time"
)

// EvictionScheduler periodically evicts idle Actors.
type EvictionScheduler[Req, Resp any] struct {
	host        *ActorHost[Req, Resp]
	idleTimeout time.Duration
	interval    time.Duration
}

// NewEvictionScheduler creates an EvictionScheduler.
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

// Start begins the eviction loop. Exits when ctx is cancelled.
func (s *EvictionScheduler[Req, Resp]) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			idleSince := time.Now().Add(-s.idleTimeout)
			for _, id := range s.host.IdleActors(idleSince) {
				if err := s.host.Evict(ctx, id); err != nil {
					// eviction failure will be retried on the next cycle
					_ = err
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// CheckpointScheduler periodically checkpoints active Actors.
// Serves as a supplement to WAL-accumulation-based automatic checkpointing.
type CheckpointScheduler[Req, Resp any] struct {
	host     *ActorHost[Req, Resp]
	interval time.Duration
}

// NewCheckpointScheduler creates a CheckpointScheduler.
func NewCheckpointScheduler[Req, Resp any](
	host *ActorHost[Req, Resp],
	interval time.Duration,
) *CheckpointScheduler[Req, Resp] {
	return &CheckpointScheduler[Req, Resp]{
		host:     host,
		interval: interval,
	}
}

// Start begins the checkpoint loop. Exits when ctx is cancelled.
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
