package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/oomymy/actorbase/provider"
)

const (
	defaultMailboxSize    = 64
	defaultFlushSize      = 32
	defaultFlushInterval  = 10 * time.Millisecond
)

// Config는 ActorHost 생성에 필요한 의존성과 설정을 담는다.
type Config[Req, Resp any] struct {
	Factory         provider.ActorFactory[Req, Resp]
	CheckpointStore provider.CheckpointStore
	WALStore        provider.WALStore
	Metrics         provider.Metrics

	MailboxSize            int           // 기본값: defaultMailboxSize
	FlushSize              int           // WAL 배치 최대 크기. 기본값: defaultFlushSize
	FlushInterval          time.Duration // WAL 배치 최대 대기 시간. 기본값: defaultFlushInterval
	CheckpointWALThreshold int           // 이 수만큼 WAL entry가 쌓이면 자동 checkpoint. 0이면 비활성.
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

// actorEntry는 활성화된 Actor 하나의 상태.
type actorEntry[Req, Resp any] struct {
	mailbox *mailbox[Req, Resp]
	ready   chan struct{} // 활성화 완료 시 close
}

// ActorHost는 파티션 서버 내 모든 Actor의 생명주기를 관리한다.
type ActorHost[Req, Resp any] struct {
	cfg     Config[Req, Resp]
	flusher *walFlusher

	mu      sync.Mutex
	actors  map[string]*actorEntry[Req, Resp]
	hostCtx context.Context
	cancel  context.CancelFunc
}

// NewActorHost는 ActorHost를 생성하고 WALFlusher를 시작한다.
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

// Send는 partitionID의 Actor에 req를 전달하고 응답을 기다린다.
// Actor가 비활성이면 먼저 활성화한다. 동시에 동일 파티션 활성화 요청이 오면 하나만 실행하고 나머지는 대기한다.
func (h *ActorHost[Req, Resp]) Send(ctx context.Context, partitionID string, req Req) (Resp, error) {
	entry, err := h.getOrActivate(ctx, partitionID)
	if err != nil {
		var zero Resp
		return zero, err
	}
	return entry.mailbox.send(ctx, req)
}

// Checkpoint는 partitionID Actor의 상태를 CheckpointStore에 저장하고 WAL을 trim한다.
// Actor는 메모리에 유지된다.
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

// Evict는 partitionID Actor를 메모리에서 제거한다.
// 제거 전 checkpoint를 수행한다.
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

// EvictAll은 모든 활성 Actor를 evict한다. 서버 종료 시 호출한다.
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
	h.cancel() // WALFlusher 종료
	return firstErr
}

// Activate는 partitionID Actor를 명시적으로 활성화한다.
// 이미 활성화된 경우 no-op. migration target PS의 PreparePartition 처리 시 사용한다.
func (h *ActorHost[Req, Resp]) Activate(ctx context.Context, partitionID string) error {
	_, err := h.getOrActivate(ctx, partitionID)
	return err
}

// Split은 partitionID Actor를 splitKey 기준으로 두 파티션으로 분리한다.
// Actor가 비활성이면 먼저 활성화한다 (checkpoint + WAL replay).
// 분리 완료 후 두 Actor 모두 활성 상태로 등록된다.
func (h *ActorHost[Req, Resp]) Split(ctx context.Context, partitionID, splitKey, newPartitionID string) error {
	entry, err := h.getOrActivate(ctx, partitionID)
	if err != nil {
		return err
	}

	// 1. Actor goroutine 내에서 split 실행
	upperHalf, err := entry.mailbox.split(ctx, splitKey)
	if err != nil {
		return fmt.Errorf("split actor: %w", err)
	}

	// 2. 하위 파티션 checkpoint (split 이후 상태)
	if err := entry.mailbox.checkpoint(ctx); err != nil {
		return fmt.Errorf("checkpoint lower half: %w", err)
	}

	// 3. 상위 파티션 checkpoint 저장 (LSN=0: WAL 없음)
	if err := h.saveRawCheckpoint(ctx, newPartitionID, 0, upperHalf); err != nil {
		return fmt.Errorf("save upper half checkpoint: %w", err)
	}

	// 4. 상위 파티션 활성화 (CheckpointStore에서 로드)
	return h.Activate(ctx, newPartitionID)
}

// IdleActors는 lastMsg가 idleSince 이전인 partitionID 목록을 반환한다.
// EvictionScheduler가 주기적으로 호출한다.
func (h *ActorHost[Req, Resp]) IdleActors(idleSince time.Time) []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	var result []string
	for id, entry := range h.actors {
		select {
		case <-entry.ready:
		default:
			continue // 아직 활성화 중
		}
		if entry.mailbox.lastMsg.Load().Before(idleSince) {
			result = append(result, id)
		}
	}
	return result
}

// ActivePartitions는 현재 활성화된 모든 partitionID 목록을 반환한다.
// CheckpointScheduler가 주기적으로 호출한다.
func (h *ActorHost[Req, Resp]) ActivePartitions() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]string, 0, len(h.actors))
	for id, entry := range h.actors {
		select {
		case <-entry.ready:
			result = append(result, id)
		default:
			// 아직 활성화 중인 경우 제외
		}
	}
	return result
}

// getOrActivate는 partitionID의 actorEntry를 반환한다.
// 없으면 activate한다. 동시 요청이 오면 하나만 activate하고 나머지는 대기한다.
func (h *ActorHost[Req, Resp]) getOrActivate(ctx context.Context, partitionID string) (*actorEntry[Req, Resp], error) {
	h.mu.Lock()
	entry, ok := h.actors[partitionID]
	if ok {
		h.mu.Unlock()
		select {
		case <-entry.ready:
			return entry, nil
		case <-ctx.Done():
			return nil, provider.ErrTimeout
		}
	}

	// 이 goroutine이 activate 담당: placeholder 등록 후 락 해제
	entry = &actorEntry[Req, Resp]{ready: make(chan struct{})}
	h.actors[partitionID] = entry
	h.mu.Unlock()

	if err := h.doActivate(ctx, partitionID, entry); err != nil {
		h.mu.Lock()
		delete(h.actors, partitionID)
		h.mu.Unlock()
		close(entry.ready) // 대기 중인 goroutine 해제
		return nil, err
	}

	close(entry.ready) // 활성화 완료 신호
	return entry, nil
}

// doActivate는 Actor를 초기화하고 mailbox를 시작한다.
func (h *ActorHost[Req, Resp]) doActivate(ctx context.Context, partitionID string, entry *actorEntry[Req, Resp]) error {
	slog.Info("activating actor", "partition_id", partitionID)

	// 1. Checkpoint 로드
	raw, err := h.cfg.CheckpointStore.Load(ctx, partitionID)
	if err != nil {
		slog.Error("activate: load checkpoint failed", "partition_id", partitionID, "err", err)
		return fmt.Errorf("load checkpoint: %w", err)
	}

	var fromLSN uint64
	var snapshotData []byte
	if len(raw) >= 8 {
		fromLSN = binary.BigEndian.Uint64(raw[:8])
		snapshotData = raw[8:]
	}
	slog.Info("activate: checkpoint loaded", "partition_id", partitionID, "lsn", fromLSN, "snapshot_bytes", len(snapshotData))

	// 2. Actor 생성 및 복원
	actor := h.cfg.Factory(partitionID)
	if len(snapshotData) > 0 {
		if err := actor.Restore(snapshotData); err != nil {
			slog.Error("activate: restore snapshot failed", "partition_id", partitionID, "err", err)
			return fmt.Errorf("restore snapshot: %w", err)
		}
	}

	// 3. WAL replay: checkpoint 이후 entry 적용.
	// fromLSN=0 (checkpoint 없음)이어도 WAL에 entry가 있으면 replay해야 한다.
	entries, err := h.cfg.WALStore.ReadFrom(ctx, partitionID, fromLSN+1)
	if err != nil {
		slog.Error("activate: read WAL failed", "partition_id", partitionID, "from_lsn", fromLSN+1, "err", err)
		return fmt.Errorf("read WAL: %w", err)
	}
	slog.Info("activate: WAL replay", "partition_id", partitionID, "entries", len(entries))
	for i, e := range entries {
		if err := actor.Replay(e.Data); err != nil {
			// 마지막 엔트리가 실패하면 crash 중 부분 기록된 partial write로 간주하고 스킵.
			// 중간 엔트리 실패는 실제 손상이므로 에러 반환.
			if i == len(entries)-1 {
				slog.Warn("activate: last WAL entry may be truncated (partial write?), skipping",
					"partition_id", partitionID, "lsn", e.LSN, "err", err)
				break
			}
			slog.Error("activate: WAL replay failed", "partition_id", partitionID, "lsn", e.LSN, "err", err)
			return fmt.Errorf("replay WAL entry LSN=%d: %w", e.LSN, err)
		}
	}

	// 4. checkpointFn 클로저: mailbox goroutine 내에서 호출되므로 actor 접근이 thread-safe
	wal := h.cfg.WALStore
	cpStore := h.cfg.CheckpointStore
	fn := checkpointFn(func(lsn uint64) error {
		snap, err := actor.Snapshot()
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		if err := saveCheckpoint(ctx, cpStore, partitionID, lsn, snap); err != nil {
			return err
		}
		return wal.TrimBefore(ctx, partitionID, lsn)
	})

	// 5. onWALError: WAL flush 실패 시 checkpoint 없이 actors 맵에서 제거
	onWALError := func() {
		slog.Error("WAL flush failed, actor evicted without checkpoint",
			"partition_id", partitionID)
		h.mu.Lock()
		delete(h.actors, partitionID)
		h.mu.Unlock()
	}

	// 6. mailbox 생성 및 goroutine 시작
	logger := slog.Default().With("partition_id", partitionID)
	mb := newMailbox[Req, Resp](
		h.cfg.MailboxSize,
		h.flusher.submitCh,
		fn,
		h.cfg.CheckpointWALThreshold,
		onWALError,
	)
	entry.mailbox = mb

	go mb.run(actor, actorCtx{partitionID: partitionID, logger: logger})
	slog.Info("actor activated", "partition_id", partitionID, "wal_replayed", len(entries))
	return nil
}

// saveRawCheckpoint는 이미 직렬화된 snapshot 데이터를 CheckpointStore에 저장한다.
// Split에서 상위 파티션 checkpoint 저장 시 사용한다.
func (h *ActorHost[Req, Resp]) saveRawCheckpoint(ctx context.Context, partitionID string, lsn uint64, snap []byte) error {
	return saveCheckpoint(ctx, h.cfg.CheckpointStore, partitionID, lsn, snap)
}

// saveCheckpoint는 lsn과 snap을 합쳐 CheckpointStore에 저장한다.
func saveCheckpoint(ctx context.Context, store provider.CheckpointStore, partitionID string, lsn uint64, snap []byte) error {
	data := make([]byte, 8+len(snap))
	binary.BigEndian.PutUint64(data[:8], lsn)
	copy(data[8:], snap)
	return store.Save(ctx, partitionID, data)
}
