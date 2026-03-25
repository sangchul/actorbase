package engine

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/sangchul/actorbase/provider"
)

// envelope은 Actor에 전달할 메시지 하나.
type envelope[Req, Resp any] struct {
	req     Req
	replyCh chan<- result[Resp]
}

// result는 처리 완료 후 caller에게 전달되는 최종 결과.
type result[Resp any] struct {
	resp Resp
	err  error
}

// checkpointReq는 외부에서 mailbox에 checkpoint를 요청할 때 사용한다.
type checkpointReq struct {
	done chan<- error
}

// exportReq는 외부에서 mailbox에 상태 내보내기를 요청할 때 사용한다.
// splitKey=""이면 전체 상태(snapshot), 비어 있지 않으면 split.
type exportReq struct {
	splitKey      string // "" = snapshot, non-empty = split
	keyRangeStart string // split 시 midpoint fallback용
	keyRangeEnd   string // split 시 midpoint fallback용
	done          chan exportResult
}

type exportResult struct {
	splitKey string // 실제 사용된 split key (snapshot이면 "")
	data     []byte
	err      error
}

// importReq는 외부에서 mailbox에 상태 읽어오기를 요청할 때 사용한다.
// restore(빈 actor)와 merge(기존 actor) 모두 이 채널을 사용한다.
type importReq struct {
	data []byte
	done chan<- error
}

// actorCtx는 provider.Context 구현체.
type actorCtx struct {
	partitionID string
	logger      *slog.Logger
}

func (c actorCtx) PartitionID() string  { return c.partitionID }
func (c actorCtx) Logger() *slog.Logger { return c.logger }

// checkpointFn은 ActorHost가 mailbox에 주입하는 checkpoint 수행 함수.
// mailbox goroutine 내에서 호출되므로 actor.Export("")이 thread-safe하게 실행된다.
type checkpointFn func(lsn uint64) error

// mailbox는 하나의 Actor에 대한 메시지 큐.
// 단일 goroutine(run)이 메시지를 순차 처리하여 Actor의 단일 스레드 실행을 보장한다.
type mailbox[Req, Resp any] struct {
	inCh     chan envelope[Req, Resp]
	exportCh chan exportReq
	importCh chan importReq
	submitCh       chan<- walPending // WALFlusher 수신 채널
	walConfirmedCh chan uint64       // WALFlusher → mailbox goroutine LSN 피드백
	// checkpointCh는 외부(ActorHost)가 checkpoint를 요청하는 채널.
	// ActorHost.Checkpoint → mailbox.checkpoint() → checkpointCh로 전달.
	// mailbox goroutine이 수신하면 drain 대기 후 Snapshot+WAL trim 수행.
	// 완료 결과는 checkpointReq.done 채널로 반환된다.
	checkpointCh chan checkpointReq

	checkpointFn checkpointFn
	walThreshold int // 0이면 WAL 누적 기반 자동 checkpoint 비활성화
	onWALError   func()

	lastMsg      atomic_time // EvictionScheduler 참조용
	confirmedLSN atomic_uint64

	rps      rpsCounter   // RPS 슬라이딩 윈도우
	keyCount atomic.Int64 // actor가 Countable이면 갱신됨. 아니면 -1 유지.

	doneCh chan struct{} // run() 종료 시 close
}

// atomic_time / atomic_uint64: sync/atomic 타입을 별도 명명 없이 인라인으로 선언하기 위한 래퍼.
// Go 1.19+ atomic.Value/atomic.Uint64를 직접 사용한다.
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
	m.keyCount.Store(-1) // Countable 미구현 시 기본값
	return m
}

// send는 req를 mailbox에 전달하고 결과를 기다린다.
// ctx 만료 시 즉시 ErrTimeout을 반환한다.
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

// checkpoint는 mailbox에 checkpoint 요청을 보내고 완료를 기다린다.
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

// export는 mailbox goroutine 내에서 Actor.Export(splitKey)를 실행한다.
// splitKey=""이면 전체 상태(snapshot), 비어 있지 않으면 split.
// split 시 실제 사용된 splitKey와 데이터를 반환한다.
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

// importData는 mailbox goroutine 내에서 Actor.Import(data)를 실행한다.
// 빈 actor에 호출하면 restore, 기존 actor에 호출하면 merge.
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

// close는 inCh를 닫아 run goroutine을 종료시킨다.
func (m *mailbox[Req, Resp]) close() {
	close(m.inCh)
}

// stats는 mailbox의 현재 통계를 반환한다.
func (m *mailbox[Req, Resp]) stats() (keyCount int64, rps float64) {
	return m.keyCount.Load(), m.rps.rps(60)
}

// run은 mailbox 이벤트 루프. 별도 goroutine으로 실행된다.
//
// 종료 조건: inCh가 close되어 ok=false를 수신하거나 WAL 오류 발생.
// doneCh는 종료 시 항상 close된다.
func (m *mailbox[Req, Resp]) run(actor provider.Actor[Req, Resp], actCtx actorCtx) {
	defer close(m.doneCh)

	pendingWAL := 0          // WALFlusher에 제출했지만 아직 미확인 write 수
	walsSinceCheckpoint := 0 // 마지막 checkpoint 이후 확인된 WAL entry 수
	dirty := false           // WAL 없이 상태가 변경된 경우 (e.g. split). checkpoint skip 방지용.

	draining := false          // true이면 inCh/splitCh 일시 중단 (checkpoint drain 대기)
	var drainDone chan<- error // 현재 drain의 완료 채널. nil이면 자동 트리거.

	doCheckpoint := func(lsn uint64) error {
		err := m.checkpointFn(lsn)
		walsSinceCheckpoint = 0
		dirty = false
		return err
	}

	for {
		// drain 중이면 새 메시지/export/import 수신 중단
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
				return // close()로 종료
			}
			resp, walEntry, err := safeReceive(actor, actCtx, env.req)
			m.rps.inc()

			if walEntry == nil || err != nil {
				env.replyCh <- result[Resp]{resp: resp, err: err}
				m.lastMsg.Store(time.Now())
				continue
			}

			// write 연산: WALFlusher에 위임하고 즉시 다음 메시지 처리
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
					walConfirmedCh <- lsn // 오류 시 0 (에러 센티널)
				},
			}
			m.lastMsg.Store(time.Now())

		case req := <-exportCh:
			splitKey := req.splitKey
			if splitKey == "" {
				// snapshot 모드: 전체 상태 내보내기 (read-only)
				data, err := safeExport(actor, actCtx, "")
				req.done <- exportResult{data: data, err: err}
			} else {
				// split 모드: SplitHinter → midpoint fallback 체인
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
					dirty = true // WAL 없이 actor 상태가 변경됨 → checkpoint skip 방지
				}
			}

		case req := <-importCh:
			err := safeImport(actor, actCtx, req.data)
			req.done <- err
			if err == nil {
				dirty = true // WAL 없이 actor 상태가 변경됨 → checkpoint skip 방지
			}

		case lsn := <-m.walConfirmedCh:
			pendingWAL--
			if c, ok := any(actor).(provider.Countable); ok {
				m.keyCount.Store(c.KeyCount())
			}

			if lsn == 0 {
				// WAL flush 실패: 메모리 상태와 WAL 불일치 → Actor evict
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
				// WAL 누적 기반 자동 checkpoint 트리거
				if pendingWAL == 0 {
					doCheckpoint(m.confirmedLSN.Load()) //nolint:errcheck
				} else {
					draining = true
					drainDone = nil // 완료 통보 불필요 (자동 트리거)
				}
			}

		case req := <-m.checkpointCh:
			if walsSinceCheckpoint == 0 && !dirty {
				// 마지막 checkpoint 이후 변경 없음 → skip
				req.done <- nil
				continue
			}
			if pendingWAL == 0 {
				req.done <- doCheckpoint(m.confirmedLSN.Load())
			} else if draining {
				// 이미 drain 중: 외부 요청으로 격상 (완료 채널 연결)
				drainDone = req.done
			} else {
				draining = true
				drainDone = req.done
			}
		}
	}
}

// safeReceive는 Actor.Receive를 호출하고 panic을 ErrActorPanicked로 변환한다.
func safeReceive[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, req Req) (resp Resp, walEntry []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Receive", "panic", r)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Receive(actCtx, req)
}

// splitKeyAuto는 split 시 SplitHinter가 힌트를 반환하지 않았을 때
// midpoint fallback을 트리거하기 위한 센티넬 값.
// export() 호출부에서 splitKey를 이 값으로 설정해야 SplitHinter 체인이 동작한다.
const splitKeyAuto = "\x00__auto__"

// safeExport는 Actor.Export를 호출하고 panic을 ErrActorPanicked로 변환한다.
func safeExport[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, splitKey string) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Export", "panic", r, "splitKey", splitKey)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Export(splitKey)
}

// safeImport는 Actor.Import를 호출하고 panic을 ErrActorPanicked로 변환한다.
func safeImport[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Import", "panic", r)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Import(data)
}
