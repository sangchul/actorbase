package engine

import (
	"time"

	"github.com/sangchul/actorbase/provider"
)

// drainState tracks the WAL-drain state machine within the mailbox goroutine.
// All methods are called exclusively from the run() goroutine; no synchronization needed.
type drainState struct {
	pending int         // WAL entries submitted but not yet confirmed
	active  bool        // true = drain in progress (inCh/exportCh/importCh masked)
	done    chan<- error // completion channel; nil = auto-triggered (no reply needed)
}

// submit increments the pending count when a WAL entry is sent to WALFlusher.
func (d *drainState) submit() { d.pending++ }

// confirm decrements the pending count. Returns true when drain is active and pending just hit zero.
func (d *drainState) confirm() bool {
	d.pending--
	return d.active && d.pending == 0
}

// start begins a drain. If already active, upgrades by connecting the done channel.
func (d *drainState) start(done chan<- error) {
	if d.active {
		d.done = done // upgrade: connect external completion channel
		return
	}
	d.active = true
	d.done = done
}

// finish sends err to the done channel (if non-nil) and resets the drain state.
func (d *drainState) finish(err error) {
	if d.done != nil {
		d.done <- err
	}
	d.active = false
	d.done = nil
}

// isIdle reports whether there are no WAL entries in flight.
func (d *drainState) isIdle() bool { return d.pending == 0 }

// isActive reports whether a drain is currently in progress.
func (d *drainState) isActive() bool { return d.active }

// run is the mailbox event loop. Executed in a separate goroutine.
//
// Termination conditions: inCh is closed (ok=false) or a WAL error occurs.
// doneCh is always closed on exit.
func (m *mailbox[Req, Resp]) run(actor provider.Actor[Req, Resp], actCtx actorCtx) {
	defer close(m.doneCh)

	var drain drainState
	walsSinceCheckpoint := 0
	dirty := false

	for {
		inCh, exportCh, importCh := m.maskedChannels(&drain)

		select {
		case env, ok := <-inCh:
			if m.handleMessage(actor, actCtx, env, ok, &drain) {
				return
			}

		case req := <-exportCh:
			if m.handleExport(actor, actCtx, req) {
				dirty = true
			}

		case req := <-importCh:
			err := safeImport(actor, actCtx, req.data)
			req.done <- err
			if err == nil {
				dirty = true
			}

		case lsn := <-m.walConfirmedCh:
			if m.handleWALConfirmed(actor, lsn, &drain, &walsSinceCheckpoint, &dirty) {
				return
			}

		case req := <-m.checkpointCh:
			m.handleCheckpoint(req, &drain, &walsSinceCheckpoint, &dirty)
		}
	}
}

// maskedChannels returns nil channels when draining (to pause message intake).
func (m *mailbox[Req, Resp]) maskedChannels(drain *drainState) (
	inCh <-chan envelope[Req, Resp],
	exportCh <-chan exportReq,
	importCh <-chan importReq,
) {
	if !drain.isActive() {
		return m.inCh, m.exportCh, m.importCh
	}
	return nil, nil, nil
}

// handleMessage processes a single inbound message from inCh.
// Returns true when inCh is closed (signals run() to terminate).
func (m *mailbox[Req, Resp]) handleMessage(
	actor provider.Actor[Req, Resp],
	actCtx actorCtx,
	env envelope[Req, Resp],
	ok bool,
	drain *drainState,
) (terminate bool) {
	if !ok {
		return true // inCh closed via close()
	}

	resp, walEntry, err := safeReceive(actor, actCtx, env.req)
	m.rps.inc()
	m.lastMsg.Store(time.Now())

	if walEntry == nil || err != nil {
		env.replyCh <- result[Resp]{resp: resp, err: err}
		return false
	}

	// Write operation: delegate to WALFlusher and immediately process next message.
	drain.submit()
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
	return false
}

// handleExport executes Actor.Export inside the mailbox goroutine.
// Returns dirty=true when actor state changed (split succeeded).
func (m *mailbox[Req, Resp]) handleExport(
	actor provider.Actor[Req, Resp],
	actCtx actorCtx,
	req exportReq,
) (dirty bool) {
	if req.splitKey == "" {
		// Snapshot mode: full state export (read-only).
		data, err := safeExport(actor, actCtx, "")
		req.done <- exportResult{data: data, err: err}
		return false
	}

	// Split mode: SplitHinter → splitKeyAuto midpoint fallback chain.
	splitKey := req.splitKey
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
	return err == nil
}

// handleWALConfirmed processes a WAL confirmation from WALFlusher.
// Returns true when a WAL error forces actor eviction (signals run() to terminate).
func (m *mailbox[Req, Resp]) handleWALConfirmed(
	actor provider.Actor[Req, Resp],
	lsn uint64,
	drain *drainState,
	walsSinceCheckpoint *int,
	dirty *bool,
) (terminate bool) {
	drainReady := drain.confirm()

	if c, ok := any(actor).(provider.Countable); ok {
		m.keyCount.Store(c.KeyCount())
	}

	if lsn == 0 {
		// WAL flush failed: in-memory state and WAL are inconsistent → evict actor.
		m.onWALError()
		return true
	}
	m.confirmedLSN.Store(lsn)
	*walsSinceCheckpoint++

	if drainReady {
		err := m.doCheckpoint(m.confirmedLSN.Load(), walsSinceCheckpoint, dirty)
		drain.finish(err)
		return false
	}

	if !drain.isActive() && m.walThreshold > 0 && *walsSinceCheckpoint >= m.walThreshold {
		m.triggerAutoCheckpoint(drain, walsSinceCheckpoint, dirty)
	}
	return false
}

// triggerAutoCheckpoint initiates or defers a WAL-accumulation-based checkpoint.
func (m *mailbox[Req, Resp]) triggerAutoCheckpoint(
	drain *drainState,
	walsSinceCheckpoint *int,
	dirty *bool,
) {
	if drain.isIdle() {
		m.doCheckpoint(m.confirmedLSN.Load(), walsSinceCheckpoint, dirty) //nolint:errcheck
	} else {
		drain.start(nil) // defer until pending WAL entries are confirmed
	}
}

// handleCheckpoint processes an external checkpoint request from ActorHost.
func (m *mailbox[Req, Resp]) handleCheckpoint(
	req checkpointReq,
	drain *drainState,
	walsSinceCheckpoint *int,
	dirty *bool,
) {
	if *walsSinceCheckpoint == 0 && !*dirty {
		req.done <- nil // no changes since last checkpoint → skip
		return
	}
	if drain.isIdle() {
		req.done <- m.doCheckpoint(m.confirmedLSN.Load(), walsSinceCheckpoint, dirty)
		return
	}
	drain.start(req.done)
}

// doCheckpoint executes the checkpoint function and resets the dirty/counter state.
func (m *mailbox[Req, Resp]) doCheckpoint(lsn uint64, walsSinceCheckpoint *int, dirty *bool) error {
	err := m.checkpointFn(lsn)
	*walsSinceCheckpoint = 0
	*dirty = false
	return err
}
