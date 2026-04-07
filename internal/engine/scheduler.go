package engine

import (
	"context"
	"time"
)

// evictionTarget is the subset of ActorHost required by EvictionScheduler.
type evictionTarget interface {
	IdleActors(idleSince time.Time) []string
	Evict(ctx context.Context, partitionID string) error
}

// checkpointTarget is the subset of ActorHost required by CheckpointScheduler.
type checkpointTarget interface {
	ActivePartitions() []string
	Checkpoint(ctx context.Context, partitionID string) error
}

// runPeriodic calls fn on every interval tick until ctx is cancelled.
func runPeriodic(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fn()
		case <-ctx.Done():
			return
		}
	}
}

// EvictionScheduler periodically evicts idle Actors.
type EvictionScheduler[Req, Resp any] struct {
	host        evictionTarget
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
	runPeriodic(ctx, s.interval, func() {
		idleSince := time.Now().Add(-s.idleTimeout)
		for _, id := range s.host.IdleActors(idleSince) {
			s.host.Evict(ctx, id) //nolint:errcheck
		}
	})
}

// CheckpointScheduler periodically checkpoints active Actors.
// Serves as a supplement to WAL-accumulation-based automatic checkpointing.
type CheckpointScheduler[Req, Resp any] struct {
	host     checkpointTarget
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
	runPeriodic(ctx, s.interval, func() {
		for _, id := range s.host.ActivePartitions() {
			s.host.Checkpoint(ctx, id) //nolint:errcheck
		}
	})
}
