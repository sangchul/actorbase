package engine

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

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

	lastMsg      atomic_time // referenced by EvictionScheduler
	confirmedLSN atomic_uint64

	rps      rpsCounter   // RPS sliding window
	keyCount atomic.Int64 // updated if actor implements Countable; otherwise stays -1.

	doneCh chan struct{} // closed when run() exits
}

// atomic_time / atomic_uint64: wrappers to use sync/atomic types inline without separate type declarations.
// Uses atomic.Value/atomic.Uint64 directly (Go 1.19+).
type atomic_time = atomicTime
type atomic_uint64 = atomicUint64

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

// run is the mailbox event loop. Executed in a separate goroutine.
//
// Termination conditions: inCh is closed (ok=false) or a WAL error occurs.
// doneCh is always closed on exit.
func (m *mailbox[Req, Resp]) run(actor provider.Actor[Req, Resp], actCtx actorCtx) {
	defer close(m.doneCh)

	pendingWAL := 0          // number of writes submitted to WALFlusher but not yet confirmed
	walsSinceCheckpoint := 0 // number of confirmed WAL entries since the last checkpoint
	dirty := false           // state was changed without a WAL entry (e.g. split); prevents skipping checkpoint

	draining := false          // when true, inCh/splitCh are paused (waiting for checkpoint drain)
	var drainDone chan<- error // completion channel for the current drain; nil means auto-triggered

	doCheckpoint := func(lsn uint64) error {
		err := m.checkpointFn(lsn)
		walsSinceCheckpoint = 0
		dirty = false
		return err
	}

	for {
		// while draining, stop receiving new messages/exports/imports
		var inCh <-chan envelope[Req, Resp]
		var exportCh <-chan exportReq
		var importCh <-chan importReq
		if !draining {
			inCh = m.inCh
			exportCh = m.exportCh
			importCh = m.importCh
		}

		select {
		case env, ok := <-inCh:
			if !ok {
				return // terminated via close()
			}
			resp, walEntry, err := safeReceive(actor, actCtx, env.req)
			m.rps.inc()

			if walEntry == nil || err != nil {
				env.replyCh <- result[Resp]{resp: resp, err: err}
				m.lastMsg.Store(time.Now())
				continue
			}

			// write operation: delegate to WALFlusher and immediately process next message
			pendingWAL++
			replyCh := env.replyCh
			walConfirmedCh := m.walConfirmedCh
			m.submitCh <- walPending{
				partitionID: actCtx.partitionID,
				entry:       walEntry,
				reply: func(lsn uint64, flushErr error) {
					if flushErr != nil {
						replyCh <- result[Resp]{err: flushErr}
					} else {
						replyCh <- result[Resp]{resp: resp}
					}
					walConfirmedCh <- lsn // 0 on error (error sentinel)
				},
			}
			m.lastMsg.Store(time.Now())

		case req := <-exportCh:
			splitKey := req.splitKey
			if splitKey == "" {
				// snapshot mode: export full state (read-only)
				data, err := safeExport(actor, actCtx, "")
				req.done <- exportResult{data: data, err: err}
			} else {
				// split mode: SplitHinter → midpoint fallback chain
				if hinter, ok := any(actor).(provider.SplitHinter); ok {
					if hint := hinter.SplitHint(); hint != "" {
						splitKey = hint
					}
				}
				if splitKey == splitKeyAuto {
					splitKey = KeyRangeMidpoint(req.keyRangeStart, req.keyRangeEnd)
				}
				data, err := safeExport(actor, actCtx, splitKey)
				req.done <- exportResult{splitKey: splitKey, data: data, err: err}
				if err == nil {
					dirty = true // actor state changed without WAL → prevent checkpoint skip
				}
			}

		case req := <-importCh:
			err := safeImport(actor, actCtx, req.data)
			req.done <- err
			if err == nil {
				dirty = true // actor state changed without WAL → prevent checkpoint skip
			}

		case lsn := <-m.walConfirmedCh:
			pendingWAL--
			if c, ok := any(actor).(provider.Countable); ok {
				m.keyCount.Store(c.KeyCount())
			}

			if lsn == 0 {
				// WAL flush failed: in-memory state and WAL are inconsistent → evict Actor
				m.onWALError()
				return
			}
			m.confirmedLSN.Store(lsn)
			walsSinceCheckpoint++

			if draining && pendingWAL == 0 {
				err := doCheckpoint(m.confirmedLSN.Load())
				if drainDone != nil {
					drainDone <- err
				}
				draining = false
				drainDone = nil
			} else if !draining && m.walThreshold > 0 && walsSinceCheckpoint >= m.walThreshold {
				// WAL accumulation-based automatic checkpoint trigger
				if pendingWAL == 0 {
					doCheckpoint(m.confirmedLSN.Load()) //nolint:errcheck
				} else {
					draining = true
					drainDone = nil // no completion notification needed (auto-triggered)
				}
			}

		case req := <-m.checkpointCh:
			if walsSinceCheckpoint == 0 && !dirty {
				// no changes since last checkpoint → skip
				req.done <- nil
				continue
			}
			if pendingWAL == 0 {
				req.done <- doCheckpoint(m.confirmedLSN.Load())
			} else if draining {
				// already draining: upgrade to external request (connect completion channel)
				drainDone = req.done
			} else {
				draining = true
				drainDone = req.done
			}
		}
	}
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
