package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sangchul/actorbase/provider"
)

const (
	defaultMailboxSize   = 64
	defaultFlushSize     = 32
	defaultFlushInterval = 10 * time.Millisecond
)

// Config holds the dependencies and settings required to create an ActorHost.
type Config[Req, Resp any] struct {
	Factory         provider.ActorFactory[Req, Resp]
	CheckpointStore provider.CheckpointStore
	WALStore        provider.WALStore
	Metrics         provider.Metrics

	MailboxSize            int           // default: defaultMailboxSize
	FlushSize              int           // maximum WAL batch size; default: defaultFlushSize
	FlushInterval          time.Duration // maximum WAL batch wait time; default: defaultFlushInterval
	CheckpointWALThreshold int           // triggers automatic checkpoint after this many WAL entries accumulate; 0 disables.
}

func (c *Config[Req, Resp]) setDefaults() {
	if c.MailboxSize <= 0 {
		c.MailboxSize = defaultMailboxSize
	}
	if c.FlushSize <= 0 {
		c.FlushSize = defaultFlushSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = defaultFlushInterval
	}
}

// checkpointRecord is the on-disk format for a checkpoint: [8-byte big-endian LSN][snapshot bytes].
// Keeping encode/decode together prevents format drift between the two sites.
type checkpointRecord struct {
	LSN      uint64
	Snapshot []byte
}

func (r checkpointRecord) marshal() []byte {
	data := make([]byte, 8+len(r.Snapshot))
	binary.BigEndian.PutUint64(data[:8], r.LSN)
	copy(data[8:], r.Snapshot)
	return data
}

func unmarshalCheckpoint(b []byte) checkpointRecord {
	if len(b) < 8 {
		return checkpointRecord{}
	}
	return checkpointRecord{
		LSN:      binary.BigEndian.Uint64(b[:8]),
		Snapshot: b[8:],
	}
}

// actorEntry holds the state of a single activated Actor.
type actorEntry[Req, Resp any] struct {
	mailbox *mailbox[Req, Resp]
	ready   chan struct{} // closed when activation is complete (or failed)
	err     error        // non-nil if activation failed; set before closing ready
}

// ActorHost manages the lifecycle of all Actors within a partition server.
type ActorHost[Req, Resp any] struct {
	cfg     Config[Req, Resp]
	flusher *walFlusher

	mu      sync.Mutex
	actors  map[string]*actorEntry[Req, Resp]
	hostCtx context.Context
	cancel  context.CancelFunc
}

// NewActorHost creates an ActorHost and starts the WALFlusher.
func NewActorHost[Req, Resp any](cfg Config[Req, Resp]) *ActorHost[Req, Resp] {
	cfg.setDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	h := &ActorHost[Req, Resp]{
		cfg:     cfg,
		flusher: newWALFlusher(cfg.WALStore, cfg.FlushSize, cfg.FlushInterval),
		actors:  make(map[string]*actorEntry[Req, Resp]),
		hostCtx: ctx,
		cancel:  cancel,
	}
	go h.flusher.start(ctx)
	return h
}

// Send delivers req to the Actor for partitionID and waits for the response.
// If the Actor is not active, it is activated first. Concurrent activation requests
// for the same partition are deduplicated: only one runs and the rest wait.
func (h *ActorHost[Req, Resp]) Send(ctx context.Context, partitionID string, req Req) (Resp, error) {
	entry, err := h.getOrActivate(ctx, partitionID)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return entry.mailbox.send(ctx, req)
}

// Checkpoint saves the state of the partitionID Actor to the CheckpointStore and trims the WAL.
// The Actor remains in memory.
func (h *ActorHost[Req, Resp]) Checkpoint(ctx context.Context, partitionID string) error {
	h.mu.Lock()
	entry, ok := h.actors[partitionID]
	h.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-entry.ready:
	case <-ctx.Done():
		return provider.ErrTimeout
	}
	return entry.mailbox.checkpoint(ctx)
}

// Evict removes the partitionID Actor from memory.
// A checkpoint is performed before removal.
func (h *ActorHost[Req, Resp]) Evict(ctx context.Context, partitionID string) error {
	h.mu.Lock()
	entry, ok := h.actors[partitionID]
	if ok {
		delete(h.actors, partitionID)
	}
	h.mu.Unlock()

	if !ok {
		return nil
	}

	select {
	case <-entry.ready:
	case <-ctx.Done():
		return provider.ErrTimeout
	}

	if err := entry.mailbox.checkpoint(ctx); err != nil {
		slog.Warn("checkpoint failed during eviction", "partition_id", partitionID, "err", err)
	}
	entry.mailbox.close()
	return nil
}

// EvictAll evicts all active Actors. Called on server shutdown.
func (h *ActorHost[Req, Resp]) EvictAll(ctx context.Context) error {
	h.mu.Lock()
	ids := make([]string, 0, len(h.actors))
	for id := range h.actors {
		ids = append(ids, id)
	}
	h.mu.Unlock()

	var firstErr error
	for _, id := range ids {
		if err := h.Evict(ctx, id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	h.cancel() // stop the WALFlusher
	return firstErr
}

// Activate explicitly activates the partitionID Actor.
// No-op if already active. Used when handling PreparePartition on the migration target PS.
func (h *ActorHost[Req, Resp]) Activate(ctx context.Context, partitionID string) error {
	_, err := h.getOrActivate(ctx, partitionID)
	return err
}

// Split splits the partitionID Actor into two partitions at splitKey.
// If the Actor is not active, it is activated first (checkpoint + WAL replay).
// After the split, both Actors are registered in the active state.
//
// If splitKey is "", the Actor's SplitHint() is used if it implements SplitHinter;
// otherwise the midpoint of keyRangeStart/End is computed.
// Returns the splitKey that was actually used.
func (h *ActorHost[Req, Resp]) Split(ctx context.Context, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error) {
	entry, err := h.getOrActivate(ctx, partitionID)
	if err != nil {
		return "", err
	}

	// 1. run split inside the Actor goroutine (includes SplitHint or midpoint resolution)
	if splitKey == "" {
		splitKey = splitKeyAuto
	}
	usedKey, upperHalf, err := entry.mailbox.export(ctx, splitKey, keyRangeStart, keyRangeEnd)
	if err != nil {
		return "", fmt.Errorf("split actor: %w", err)
	}

	// 2. checkpoint the lower partition (state after split)
	if err := entry.mailbox.checkpoint(ctx); err != nil {
		return "", fmt.Errorf("checkpoint lower half: %w", err)
	}

	// 3. save checkpoint for the upper partition (LSN=0: no WAL)
	if err := h.saveRawCheckpoint(ctx, newPartitionID, 0, upperHalf); err != nil {
		return "", fmt.Errorf("save upper half checkpoint: %w", err)
	}

	// 4. activate the upper partition (loaded from CheckpointStore)
	return usedKey, h.Activate(ctx, newPartitionID)
}

// Merge causes the lowerPartitionID Actor to absorb the state of the upperPartitionID Actor.
// Both partitions must reside on the same Host.
// After merging, the upper partition is evicted and its checkpoint/WAL are deleted.
func (h *ActorHost[Req, Resp]) Merge(ctx context.Context, lowerPartitionID, upperPartitionID string) error {
	// 1. activate upper actor → Export (full state)
	upperEntry, err := h.getOrActivate(ctx, upperPartitionID)
	if err != nil {
		return fmt.Errorf("activate upper partition: %w", err)
	}
	_, upperData, err := upperEntry.mailbox.export(ctx, "", "", "")
	if err != nil {
		return fmt.Errorf("snapshot upper partition: %w", err)
	}

	// 2. evict upper actor (delete without checkpoint)
	h.mu.Lock()
	delete(h.actors, upperPartitionID)
	h.mu.Unlock()
	upperEntry.mailbox.close()

	// 3. delete upper checkpoint/WAL
	if err := h.cfg.CheckpointStore.Delete(ctx, upperPartitionID); err != nil {
		slog.Warn("merge: delete upper checkpoint failed", "partition", upperPartitionID, "err", err)
	}
	h.cfg.WALStore.TrimBefore(ctx, upperPartitionID, ^uint64(0))

	// 4. activate lower actor → Import(upperData) → checkpoint
	lowerEntry, err := h.getOrActivate(ctx, lowerPartitionID)
	if err != nil {
		return fmt.Errorf("activate lower partition: %w", err)
	}
	if err := lowerEntry.mailbox.importData(ctx, upperData); err != nil {
		return fmt.Errorf("merge actor: %w", err)
	}
	if err := lowerEntry.mailbox.checkpoint(ctx); err != nil {
		return fmt.Errorf("checkpoint merged partition: %w", err)
	}
	return nil
}

// GetStats returns statistics for all currently active partitions.
func (h *ActorHost[Req, Resp]) GetStats() []PartitionStats {
	h.mu.Lock()
	snapshot := make(map[string]*actorEntry[Req, Resp], len(h.actors))
	for id, e := range h.actors {
		snapshot[id] = e
	}
	h.mu.Unlock()

	result := make([]PartitionStats, 0, len(snapshot))
	for id, entry := range snapshot {
		select {
		case <-entry.ready:
		default:
			continue // still activating
		}
		keyCount, rps := entry.mailbox.stats()
		result = append(result, PartitionStats{
			PartitionID: id,
			KeyCount:    keyCount,
			RPS:         rps,
		})
	}
	return result
}

// IdleActors returns the list of partitionIDs whose lastMsg is before idleSince.
// Called periodically by EvictionScheduler.
func (h *ActorHost[Req, Resp]) IdleActors(idleSince time.Time) []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	var result []string
	for id, entry := range h.actors {
		select {
		case <-entry.ready:
		default:
			continue // still activating
		}
		if entry.mailbox.lastMsg.Load().Before(idleSince) {
			result = append(result, id)
		}
	}
	return result
}

// ActivePartitions returns the list of all currently active partitionIDs.
// Called periodically by CheckpointScheduler.
func (h *ActorHost[Req, Resp]) ActivePartitions() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]string, 0, len(h.actors))
	for id, entry := range h.actors {
		select {
		case <-entry.ready:
			result = append(result, id)
		default:
			// exclude entries that are still activating
		}
	}
	return result
}

// getOrActivate returns the actorEntry for partitionID.
// If it does not exist, it activates the Actor. Concurrent requests are deduplicated:
// only one activation runs and the rest wait.
func (h *ActorHost[Req, Resp]) getOrActivate(ctx context.Context, partitionID string) (*actorEntry[Req, Resp], error) {
	h.mu.Lock()
	entry, ok := h.actors[partitionID]
	if ok {
		h.mu.Unlock()
		select {
		case <-entry.ready:
			if entry.err != nil {
				return nil, entry.err
			}
			return entry, nil
		case <-ctx.Done():
			return nil, provider.ErrTimeout
		}
	}

	// This goroutine is responsible for activation: register placeholder then release lock.
	entry = &actorEntry[Req, Resp]{ready: make(chan struct{})}
	h.actors[partitionID] = entry
	h.mu.Unlock()

	if err := h.doActivate(ctx, partitionID, entry); err != nil {
		entry.err = err // set before closing so waiters observe the error
		h.mu.Lock()
		delete(h.actors, partitionID)
		h.mu.Unlock()
		close(entry.ready)
		return nil, err
	}

	close(entry.ready) // signal activation complete
	return entry, nil
}

// doActivate initializes an Actor and starts its mailbox.
func (h *ActorHost[Req, Resp]) doActivate(ctx context.Context, partitionID string, entry *actorEntry[Req, Resp]) error {
	slog.Info("activating actor", "partition_id", partitionID)

	actor := h.cfg.Factory(partitionID)

	lsn, err := h.restoreFromCheckpoint(ctx, actor, partitionID)
	if err != nil {
		return err
	}
	if err := h.replayWAL(ctx, actor, partitionID, lsn); err != nil {
		return err
	}

	logger := slog.Default().With("partition_id", partitionID)
	mb := newMailbox[Req, Resp](
		h.cfg.MailboxSize,
		h.flusher.submitCh,
		h.makeCheckpointFn(actor, partitionID),
		h.cfg.CheckpointWALThreshold,
		h.makeOnWALError(partitionID),
	)
	entry.mailbox = mb
	go mb.run(actor, actorCtx{partitionID: partitionID, logger: logger})
	slog.Info("actor activated", "partition_id", partitionID)
	return nil
}

// restoreFromCheckpoint loads the latest checkpoint and applies it to actor.
// Returns the LSN recorded in the checkpoint (WAL replay should start from LSN+1).
func (h *ActorHost[Req, Resp]) restoreFromCheckpoint(ctx context.Context, actor provider.Actor[Req, Resp], partitionID string) (uint64, error) {
	raw, err := h.cfg.CheckpointStore.Load(ctx, partitionID)
	if err != nil {
		slog.Error("activate: load checkpoint failed", "partition_id", partitionID, "err", err)
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}

	rec := unmarshalCheckpoint(raw)
	slog.Info("activate: checkpoint loaded", "partition_id", partitionID, "lsn", rec.LSN, "snapshot_bytes", len(rec.Snapshot))

	if len(rec.Snapshot) > 0 {
		if err := actor.Import(rec.Snapshot); err != nil {
			slog.Error("activate: restore snapshot failed", "partition_id", partitionID, "err", err)
			return 0, fmt.Errorf("restore snapshot: %w", err)
		}
	}
	return rec.LSN, nil
}

// replayWAL applies WAL entries after fromLSN to actor.
func (h *ActorHost[Req, Resp]) replayWAL(ctx context.Context, actor provider.Actor[Req, Resp], partitionID string, fromLSN uint64) error {
	entries, err := h.cfg.WALStore.ReadFrom(ctx, partitionID, fromLSN+1)
	if err != nil {
		slog.Error("activate: read WAL failed", "partition_id", partitionID, "from_lsn", fromLSN+1, "err", err)
		return fmt.Errorf("read WAL: %w", err)
	}
	slog.Info("activate: WAL replay", "partition_id", partitionID, "entries", len(entries))
	for i, e := range entries {
		if err := actor.Replay(e.Data); err != nil {
			// If the last entry fails, treat it as a partial write during a crash and skip it.
			// Failure on a non-final entry indicates real data corruption; return an error.
			if i == len(entries)-1 {
				slog.Warn("activate: last WAL entry may be truncated (partial write?), skipping",
					"partition_id", partitionID, "lsn", e.LSN, "err", err)
				break
			}
			slog.Error("activate: WAL replay failed", "partition_id", partitionID, "lsn", e.LSN, "err", err)
			return fmt.Errorf("replay WAL entry LSN=%d: %w", e.LSN, err)
		}
	}
	return nil
}

// makeCheckpointFn returns the checkpoint function used by the mailbox goroutine.
// It captures actor and stores by value so the closure remains valid after doActivate returns.
// context.Background() is used intentionally: the mailbox outlives any activation request context.
func (h *ActorHost[Req, Resp]) makeCheckpointFn(actor provider.Actor[Req, Resp], partitionID string) checkpointFn {
	wal := h.cfg.WALStore
	cpStore := h.cfg.CheckpointStore
	return func(lsn uint64) error {
		snap, err := actor.Export("")
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		bgCtx := context.Background()
		if err := saveCheckpoint(bgCtx, cpStore, partitionID, checkpointRecord{LSN: lsn, Snapshot: snap}); err != nil {
			return err
		}
		return wal.TrimBefore(bgCtx, partitionID, lsn)
	}
}

// makeOnWALError returns the WAL error callback used by the mailbox goroutine.
// On WAL flush failure the actor is removed from the map without checkpointing.
func (h *ActorHost[Req, Resp]) makeOnWALError(partitionID string) func() {
	return func() {
		slog.Error("WAL flush failed, actor evicted without checkpoint", "partition_id", partitionID)
		h.mu.Lock()
		delete(h.actors, partitionID)
		h.mu.Unlock()
	}
}

// saveRawCheckpoint saves already-serialized snapshot data to the CheckpointStore.
// Used in Split to save the upper partition's checkpoint.
func (h *ActorHost[Req, Resp]) saveRawCheckpoint(ctx context.Context, partitionID string, lsn uint64, snap []byte) error {
	return saveCheckpoint(ctx, h.cfg.CheckpointStore, partitionID, checkpointRecord{LSN: lsn, Snapshot: snap})
}

// saveCheckpoint serializes rec and persists it to the CheckpointStore.
func saveCheckpoint(ctx context.Context, store provider.CheckpointStore, partitionID string, rec checkpointRecord) error {
	return store.Save(ctx, partitionID, rec.marshal())
}
