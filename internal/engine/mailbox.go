package engine

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/sangchul/actorbase/provider"
)

// envelope is a single message to be delivered to an Actor.
type envelope[Req, Resp any] struct {
	req     Req
	replyCh chan<- result[Resp]
}

// result is the final outcome delivered back to the caller after processing.
type result[Resp any] struct {
	resp Resp
	err  error
}

// checkpointReq is used to request a checkpoint from outside the mailbox.
type checkpointReq struct {
	done chan<- error
}

// exportReq is used to request a state export from outside the mailbox.
// splitKey="" means a full snapshot (read-only); non-empty means a split.
type exportReq struct {
	splitKey      string // "" = snapshot, non-empty = split
	keyRangeStart string // used for midpoint fallback during split
	keyRangeEnd   string // used for midpoint fallback during split
	done          chan exportResult
}

type exportResult struct {
	splitKey string // the splitKey actually used ("" for a snapshot)
	data     []byte
	err      error
}

// importReq is used to request a state import from outside the mailbox.
// Used for both restore (empty actor) and merge (existing actor).
type importReq struct {
	data []byte
	done chan<- error
}

// actorCtx is the implementation of provider.Context.
type actorCtx struct {
	partitionID string
	logger      *slog.Logger
}

func (c actorCtx) PartitionID() string  { return c.partitionID }
func (c actorCtx) Logger() *slog.Logger { return c.logger }

// checkpointFn is the checkpoint function injected into the mailbox by ActorHost.
// Since it is called from within the mailbox goroutine, actor.Export("") is executed thread-safely.
type checkpointFn func(lsn uint64) error

// mailbox is the message queue for a single Actor.
// A single goroutine (run) processes messages sequentially, guaranteeing single-threaded Actor execution.
type mailbox[Req, Resp any] struct {
	inCh     chan envelope[Req, Resp]
	exportCh chan exportReq
	importCh chan importReq
	submitCh       chan<- walPending // WALFlusher receive channel
	walConfirmedCh chan uint64       // LSN feedback from WALFlusher to mailbox goroutine
	// checkpointCh is the channel through which external callers (ActorHost) request a checkpoint.
	// ActorHost.Checkpoint → mailbox.checkpoint() → sent to checkpointCh.
	// When the mailbox goroutine receives it, it waits for drain then performs Snapshot + WAL trim.
	// The result is returned via checkpointReq.done.
	checkpointCh chan checkpointReq

	checkpointFn checkpointFn
	walThreshold int // 0 disables WAL-accumulation-based automatic checkpoint
	onWALError   func()

	lastMsg      atomicTime   // referenced by EvictionScheduler
	confirmedLSN atomic.Uint64

	rps      rpsCounter   // RPS sliding window
	keyCount atomic.Int64 // updated if actor implements Countable; otherwise stays -1.

	doneCh chan struct{} // closed when run() exits
}

func newMailbox[Req, Resp any](
	inSize int,
	submitCh chan<- walPending,
	fn checkpointFn,
	walThreshold int,
	onWALError func(),
) *mailbox[Req, Resp] {
	m := &mailbox[Req, Resp]{
		inCh:           make(chan envelope[Req, Resp], inSize),
		exportCh:       make(chan exportReq, 1),
		importCh:       make(chan importReq, 1),
		submitCh:       submitCh,
		walConfirmedCh: make(chan uint64, inSize+1),
		checkpointCh:   make(chan checkpointReq, 1),
		checkpointFn:   fn,
		walThreshold:   walThreshold,
		onWALError:     onWALError,
		doneCh:         make(chan struct{}),
	}
	m.keyCount.Store(-1) // default when Countable is not implemented
	return m
}

// send delivers req to the mailbox and waits for the result.
// Returns ErrTimeout immediately if ctx expires.
func (m *mailbox[Req, Resp]) send(ctx context.Context, req Req) (Resp, error) {
	replyCh := make(chan result[Resp], 1)
	select {
	case m.inCh <- envelope[Req, Resp]{req: req, replyCh: replyCh}:
	case <-m.doneCh:
		var zero Resp
		return zero, provider.ErrPartitionNotOwned
	case <-ctx.Done():
		var zero Resp
		return zero, provider.ErrTimeout
	}
	select {
	case r := <-replyCh:
		return r.resp, r.err
	case <-m.doneCh:
		var zero Resp
		return zero, provider.ErrPartitionNotOwned
	case <-ctx.Done():
		var zero Resp
		return zero, provider.ErrTimeout
	}
}

// checkpoint sends a checkpoint request to the mailbox and waits for completion.
func (m *mailbox[Req, Resp]) checkpoint(ctx context.Context) error {
	done := make(chan error, 1)
	select {
	case m.checkpointCh <- checkpointReq{done: done}:
	case <-m.doneCh:
		return nil
	case <-ctx.Done():
		return provider.ErrTimeout
	}
	select {
	case err := <-done:
		return err
	case <-m.doneCh:
		return nil
	case <-ctx.Done():
		return provider.ErrTimeout
	}
}

// export executes Actor.Export(splitKey) inside the mailbox goroutine.
// splitKey="" means a full snapshot; non-empty means a split.
// On split, returns the splitKey actually used along with the data.
func (m *mailbox[Req, Resp]) export(ctx context.Context, splitKey, keyRangeStart, keyRangeEnd string) (string, []byte, error) {
	done := make(chan exportResult, 1)
	select {
	case m.exportCh <- exportReq{splitKey: splitKey, keyRangeStart: keyRangeStart, keyRangeEnd: keyRangeEnd, done: done}:
	case <-m.doneCh:
		return "", nil, provider.ErrPartitionNotOwned
	case <-ctx.Done():
		return "", nil, provider.ErrTimeout
	}
	select {
	case res := <-done:
		return res.splitKey, res.data, res.err
	case <-m.doneCh:
		return "", nil, provider.ErrPartitionNotOwned
	case <-ctx.Done():
		return "", nil, provider.ErrTimeout
	}
}

// importData executes Actor.Import(data) inside the mailbox goroutine.
// Calling on an empty actor performs a restore; calling on an existing actor performs a merge.
func (m *mailbox[Req, Resp]) importData(ctx context.Context, data []byte) error {
	done := make(chan error, 1)
	select {
	case m.importCh <- importReq{data: data, done: done}:
	case <-m.doneCh:
		return provider.ErrPartitionNotOwned
	case <-ctx.Done():
		return provider.ErrTimeout
	}
	select {
	case err := <-done:
		return err
	case <-m.doneCh:
		return provider.ErrPartitionNotOwned
	case <-ctx.Done():
		return provider.ErrTimeout
	}
}

// close closes inCh to terminate the run goroutine.
func (m *mailbox[Req, Resp]) close() {
	close(m.inCh)
}

// stats returns the current statistics of the mailbox.
func (m *mailbox[Req, Resp]) stats() (keyCount int64, rps float64) {
	return m.keyCount.Load(), m.rps.rps(60)
}

// safeReceive calls Actor.Receive and converts any panic to ErrActorPanicked.
func safeReceive[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, req Req) (resp Resp, walEntry []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Receive", "panic", r)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Receive(actCtx, req)
}

// splitKeyAuto is a sentinel value used to trigger the midpoint fallback
// when SplitHinter does not return a hint during a split.
// The caller of export() must set splitKey to this value for the SplitHinter chain to work.
const splitKeyAuto = "\x00__auto__"

// safeExport calls Actor.Export and converts any panic to ErrActorPanicked.
func safeExport[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, splitKey string) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Export", "panic", r, "splitKey", splitKey)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Export(splitKey)
}

// safeImport calls Actor.Import and converts any panic to ErrActorPanicked.
func safeImport[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Import", "panic", r)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Import(data)
}
