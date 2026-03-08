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

		// partitionID별로 그룹화: 순서를 보존하기 위해 첫 등장 순서로 정렬된 슬라이스 사용
		type group struct {
			data    [][]byte
			indices []int // pending 슬라이스에서의 원래 위치
		}
		order := make([]string, 0, len(pending))
		groups := make(map[string]*group, len(pending))
		for i, p := range pending {
			g, exists := groups[p.partitionID]
			if !exists {
				g = &group{}
				groups[p.partitionID] = g
				order = append(order, p.partitionID)
			}
			g.data = append(g.data, p.entry)
			g.indices = append(g.indices, i)
		}

		for _, partitionID := range order {
			g := groups[partitionID]
			lsns, err := f.walStore.AppendBatch(ctx, partitionID, g.data)
			for j, idx := range g.indices {
				if err != nil {
					pending[idx].reply(0, err)
				} else {
					pending[idx].reply(lsns[j], nil)
				}
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
