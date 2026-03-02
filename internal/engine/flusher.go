package engine

import (
	"context"
	"time"

	"github.com/oomymy/actorbase/provider"
)

// walPending은 WAL flush 대기 중인 항목 하나.
// mailbox goroutine이 생성하고 WALFlusher가 소비한다.
type walPending struct {
	partitionID string
	entry       []byte
	// reply는 flush 완료 후 호출된다.
	// 성공 시 lsn > 0, 실패 시 lsn == 0 (에러 센티널).
	reply func(lsn uint64, err error)
}

// walFlusher는 여러 Actor의 WAL entry를 모아 배치로 WALStore에 기록한다.
type walFlusher struct {
	walStore      provider.WALStore
	submitCh      chan walPending
	flushSize     int
	flushInterval time.Duration
}

func newWALFlusher(walStore provider.WALStore, flushSize int, flushInterval time.Duration) *walFlusher {
	return &walFlusher{
		walStore:      walStore,
		submitCh:      make(chan walPending, flushSize*2),
		flushSize:     flushSize,
		flushInterval: flushInterval,
	}
}

// start는 flush 루프를 시작한다. ctx 취소 시 남은 항목을 모두 flush하고 종료한다.
func (f *walFlusher) start(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	pending := make([]walPending, 0, f.flushSize)

	flush := func() {
		if len(pending) == 0 {
			return
		}
		for _, p := range pending {
			lsn, err := f.walStore.Append(ctx, p.partitionID, p.entry)
			if err != nil {
				p.reply(0, err)
			} else {
				p.reply(lsn, nil)
			}
		}
		pending = pending[:0]
	}

	for {
		select {
		case p := <-f.submitCh:
			pending = append(pending, p)
			if len(pending) >= f.flushSize {
				flush()
				ticker.Reset(f.flushInterval)
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// 남은 항목 모두 처리 후 종료
		drain:
			for {
				select {
				case p := <-f.submitCh:
					pending = append(pending, p)
				default:
					break drain
				}
			}
			flush()
			return
		}
	}
}
