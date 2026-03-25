package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/sangchul/actorbase/sdk"
)

// Stats holds aggregate statistics across all workers.
type Stats struct {
	success atomic.Int64
	fail    atomic.Int64
}

// workerConfig configures the operation ratios.
type workerConfig struct {
	setRatio float64 // fraction of set operations
	delRatio float64 // fraction of del operations (remainder is get)
}

// worker performs set/del/get operations on its assigned key range ([keyLow, keyHigh)).
// The ledger is updated after a successful server response.
type worker struct {
	id      int
	keyLow  int
	keyHigh int
	client  *sdk.Client[KVRequest, KVResponse]
	ledger  *Ledger
	cfg     workerConfig
}

// run repeatedly performs set/del/get until stopCh is closed.
// ctx is used only for gRPC calls — it must remain alive until in-flight requests complete.
// When stopCh is closed, no new requests are started; the current in-flight request waits for completion via ctx.
func (w *worker) run(ctx context.Context, stopCh <-chan struct{}, stats *Stats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w.id)*1000))
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		keyIdx := w.keyLow + rng.Intn(w.keyHigh-w.keyLow)
		key := fmt.Sprintf("key:%08d", keyIdx)

		r := rng.Float64()
		switch {
		case r < w.cfg.setRatio:
			value := fmt.Sprintf("v%d-%d", keyIdx, time.Now().UnixNano())
			_, err := w.client.Send(ctx, key, KVRequest{Op: "set", Key: key, Value: []byte(value)})
			if err == nil {
				w.ledger.Set(key, value)
				stats.success.Add(1)
			} else {
				stats.fail.Add(1)
			}

		case r < w.cfg.setRatio+w.cfg.delRatio:
			_, err := w.client.Send(ctx, key, KVRequest{Op: "del", Key: key})
			if err == nil {
				w.ledger.Del(key)
				stats.success.Add(1)
			} else {
				stats.fail.Add(1)
			}

		default:
			// get — for live consistency checking only; does not update the ledger
			_, err := w.client.Send(ctx, key, KVRequest{Op: "get", Key: key})
			if err == nil {
				stats.success.Add(1)
			} else {
				stats.fail.Add(1)
			}
		}
	}
}
