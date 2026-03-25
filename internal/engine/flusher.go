package engine

import (
	"context"
	"time"

	"github.com/sangchul/actorbase/provider"
)

// walPending is a single item waiting to be WAL-flushed.
// Created by the mailbox goroutine and consumed by WALFlusher.
type walPending struct {
	partitionID string
	entry       []byte
	// reply is called after the flush completes.
	// On success, lsn > 0; on failure, lsn == 0 (error sentinel).
	reply func(lsn uint64, err error)
}

// walFlusher batches WAL entries from multiple Actors and writes them to WALStore.
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

// start begins the flush loop. On ctx cancellation, flushes all remaining items then exits.
func (f *walFlusher) start(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	pending := make([]walPending, 0, f.flushSize)

	flush := func() {
		if len(pending) == 0 {
			return
		}

		// group by partitionID; use a slice ordered by first appearance to preserve order
		type group struct {
			data    [][]byte
			indices []int // original positions in the pending slice
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
			// drain all remaining items before exiting
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
