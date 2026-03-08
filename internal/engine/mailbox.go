package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/oomymy/actorbase/provider"
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

// splitReq는 외부에서 mailbox에 split을 요청할 때 사용한다.
type splitReq struct {
	splitKey string
	done     chan splitResult
}

type splitResult struct {
	upperHalf []byte
	err       error
}

// actorCtx는 provider.Context 구현체.
type actorCtx struct {
	partitionID string
	logger      *slog.Logger
}

func (c actorCtx) PartitionID() string  { return c.partitionID }
func (c actorCtx) Logger() *slog.Logger { return c.logger }

// checkpointFn은 ActorHost가 mailbox에 주입하는 checkpoint 수행 함수.
// mailbox goroutine 내에서 호출되므로 actor.Snapshot()이 thread-safe하게 실행된다.
type checkpointFn func(lsn uint64) error

// mailbox는 하나의 Actor에 대한 메시지 큐.
// 단일 goroutine(run)이 메시지를 순차 처리하여 Actor의 단일 스레드 실행을 보장한다.
type mailbox[Req, Resp any] struct {
	inCh           chan envelope[Req, Resp]
	splitCh        chan splitReq
	submitCh       chan<- walPending // WALFlusher 수신 채널
	walConfirmedCh chan uint64 // WALFlusher → mailbox goroutine LSN 피드백
	// checkpointCh는 외부(ActorHost)가 checkpoint를 요청하는 채널.
	// ActorHost.Checkpoint → mailbox.checkpoint() → checkpointCh로 전달.
	// mailbox goroutine이 수신하면 drain 대기 후 Snapshot+WAL trim 수행.
	// 완료 결과는 checkpointReq.done 채널로 반환된다.
	checkpointCh chan checkpointReq

	checkpointFn checkpointFn
	walThreshold int // 0이면 WAL 누적 기반 자동 checkpoint 비활성화
	onWALError   func()

	lastMsg      atomic_time  // EvictionScheduler 참조용
	confirmedLSN atomic_uint64

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
	return &mailbox[Req, Resp]{
		inCh:           make(chan envelope[Req, Resp], inSize),
		splitCh:        make(chan splitReq, 1),
		submitCh:       submitCh,
		walConfirmedCh: make(chan uint64, inSize+1),
		checkpointCh:   make(chan checkpointReq, 1),
		checkpointFn:   fn,
		walThreshold:   walThreshold,
		onWALError:     onWALError,
		doneCh:         make(chan struct{}),
	}
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

// split은 mailbox에 split 요청을 보내고 결과를 기다린다.
func (m *mailbox[Req, Resp]) split(ctx context.Context, splitKey string) ([]byte, error) {
	done := make(chan splitResult, 1)
	select {
	case m.splitCh <- splitReq{splitKey: splitKey, done: done}:
	case <-m.doneCh:
		return nil, provider.ErrPartitionNotOwned
	case <-ctx.Done():
		return nil, provider.ErrTimeout
	}
	select {
	case res := <-done:
		return res.upperHalf, res.err
	case <-m.doneCh:
		return nil, provider.ErrPartitionNotOwned
	case <-ctx.Done():
		return nil, provider.ErrTimeout
	}
}

// close는 inCh를 닫아 run goroutine을 종료시킨다.
func (m *mailbox[Req, Resp]) close() {
	close(m.inCh)
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

	draining := false         // true이면 inCh/splitCh 일시 중단 (checkpoint drain 대기)
	var drainDone chan<- error // 현재 drain의 완료 채널. nil이면 자동 트리거.

	doCheckpoint := func(lsn uint64) error {
		err := m.checkpointFn(lsn)
		walsSinceCheckpoint = 0
		dirty = false
		return err
	}

	for {
		// drain 중이면 새 메시지/split 수신 중단
		var inCh <-chan envelope[Req, Resp]
		var splitCh <-chan splitReq
		if !draining {
			inCh = m.inCh
			splitCh = m.splitCh
		}

		select {
		case env, ok := <-inCh:
			if !ok {
				return // close()로 종료
			}
			resp, walEntry, err := safeReceive(actor, actCtx, env.req)

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

		case req := <-splitCh:
			upperHalf, err := safeSplit(actor, actCtx, req.splitKey)
			req.done <- splitResult{upperHalf: upperHalf, err: err}
			if err == nil {
				dirty = true // WAL 없이 actor 상태가 변경됨 → checkpoint skip 방지
			}

		case lsn := <-m.walConfirmedCh:
			pendingWAL--

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

// safeSplit은 Actor.Split을 호출하고 panic을 ErrActorPanicked로 변환한다.
func safeSplit[Req, Resp any](actor provider.Actor[Req, Resp], actCtx actorCtx, splitKey string) (upperHalf []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			actCtx.logger.Error("actor panicked in Split", "panic", r)
			err = provider.ErrActorPanicked
		}
	}()
	return actor.Split(splitKey)
}
