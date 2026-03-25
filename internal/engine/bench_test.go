package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sangchul/actorbase/provider"
)

// suppressLogs redirects slog output to /dev/null during benchmark runs.
// Activation/deactivation log lines would otherwise add noise to benchmark results.
func suppressLogs(b *testing.B) {
	b.Helper()
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(orig) })
}

// ── Actor for benchmarks ──────────────────────────────────────────────────────
//
// nopActor eliminates business-logic overhead from the actor itself, so that
// only the engine's pure processing cost (mailbox + WAL group commit) is measured.

type nopActor struct{}

func newNopActor(_ string) provider.Actor[kvReq, kvResp] { return &nopActor{} }

func (a *nopActor) Receive(_ provider.Context, req kvReq) (kvResp, []byte, error) {
	if req.Op == "write" {
		// non-nil walEntry → takes the WAL group commit path
		return kvResp{Value: "ok"}, []byte("w"), nil
	}
	// nil walEntry → responds immediately without WAL
	return kvResp{Value: "ok"}, nil, nil
}
func (a *nopActor) Replay(_ []byte) error              { return nil }
func (a *nopActor) Export(_ string) ([]byte, error)    { return []byte("{}"), nil }
func (a *nopActor) Import(_ []byte) error              { return nil }

// ── WAL Store for benchmarks ──────────────────────────────────────────────────
//
// slowWALStore injects a fixed delay per AppendBatch call (= one flush)
// to simulate SSD fsync latency.
//
// Key point: the delay is charged per batch, not per entry.
// With N concurrent senders, up to N entries can be coalesced into one batch,
// so effective throughput scales close to N× as concurrency increases.
// This is the benefit of group commit.

type slowWALStore struct {
	*memWALStore
	latency time.Duration
}

func newSlowWALStore(latency time.Duration) *slowWALStore {
	return &slowWALStore{memWALStore: newMemWALStore(), latency: latency}
}

func (s *slowWALStore) AppendBatch(ctx context.Context, partitionID string, data [][]byte) ([]uint64, error) {
	time.Sleep(s.latency)
	return s.memWALStore.AppendBatch(ctx, partitionID, data)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const benchPartition = "bench-p1"

func newBenchHost(b *testing.B, wal provider.WALStore, flushSize int, flushInterval time.Duration) *ActorHost[kvReq, kvResp] {
	b.Helper()
	return NewActorHost(Config[kvReq, kvResp]{
		Factory:         newNopActor,
		WALStore:        wal,
		CheckpointStore: newMemCheckpointStore(),
		FlushSize:       flushSize,
		FlushInterval:   flushInterval,
		MailboxSize:     512,
	})
}

func warmup(b *testing.B, host *ActorHost[kvReq, kvResp]) {
	b.Helper()
	ctx := context.Background()
	// complete actor activation (WAL replay + checkpoint load) before measurement starts
	if _, err := host.Send(ctx, benchPartition, kvReq{Op: "read"}); err != nil {
		b.Fatalf("warmup: %v", err)
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkActorHost_Read measures read throughput without WAL.
//
// walEntry=nil path: the caller receives a response as soon as actor.Receive returns.
// This number is the upper bound of the engine's processing speed.
// (includes only mailbox channel send/recv + goroutine context switch cost)
func BenchmarkActorHost_Read(b *testing.B) {
	suppressLogs(b)
	host := newBenchHost(b, newMemWALStore(), 256, 5*time.Millisecond)
	ctx := context.Background()
	warmup(b, host)

	b.ResetTimer()
	for b.Loop() {
		if _, err := host.Send(ctx, benchPartition, kvReq{Op: "read"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")

	b.StopTimer()
	host.EvictAll(ctx) //nolint:errcheck
}

// BenchmarkActorHost_Write_MemWAL measures write throughput using an in-memory WAL.
//
// It exercises the group commit path (actor → flusher → reply), but WAL I/O itself
// is in memory so latency is negligible. The drop compared to Read reflects the
// overhead of the group commit path alone.
func BenchmarkActorHost_Write_MemWAL(b *testing.B) {
	suppressLogs(b)
	host := newBenchHost(b, newMemWALStore(), 256, 5*time.Millisecond)
	ctx := context.Background()
	warmup(b, host)

	b.ResetTimer()
	for b.Loop() {
		if _, err := host.Send(ctx, benchPartition, kvReq{Op: "write"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")

	b.StopTimer()
	host.EvictAll(ctx) //nolint:errcheck
}

// BenchmarkActorHost_Write_SlowWAL simulates disk fsync latency (500µs/batch)
// and measures how throughput changes with the number of concurrent senders.
//
// Key comparison:
//   - concurrency=1 (sequential): WAL latency is fully exposed → ~1/latency ops/s
//   - concurrency=N (parallel):   N requests are coalesced into one batch → ~N/latency ops/s
//
// This illustrates how group commit scales write throughput for a single-threaded actor.
// The actor goroutine processes the next message immediately without waiting for WAL
// completion, so WAL latency affects latency but not throughput.
func BenchmarkActorHost_Write_SlowWAL(b *testing.B) {
	const (
		walLatency    = 500 * time.Microsecond // typical SSD fsync level
		flushInterval = 10 * time.Millisecond
		flushSize     = 256
	)

	for _, concurrency := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			suppressLogs(b)
			host := newBenchHost(b, newSlowWALStore(walLatency), flushSize, flushInterval)
			ctx := context.Background()
			warmup(b, host)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := host.Send(ctx, benchPartition, kvReq{Op: "write"}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")

			b.StopTimer()
			host.EvictAll(ctx) //nolint:errcheck
		})
	}
}

// BenchmarkActorHost_FlushInterval compares write throughput across FlushInterval settings.
//
// Smaller FlushInterval:
//   - lower latency (callers receive responses sooner)
//   - smaller batch size → more WAL I/O calls → lower throughput
//
// Larger FlushInterval:
//   - higher throughput (more entries coalesced into one batch)
//   - higher latency (callers wait longer)
//
// These numbers help choose a FlushInterval suited to the disk characteristics of the deployment environment.
func BenchmarkActorHost_FlushInterval(b *testing.B) {
	const (
		walLatency  = 500 * time.Microsecond
		concurrency = 16
		flushSize   = 256
	)

	for _, interval := range []time.Duration{
		1 * time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
	} {
		b.Run(fmt.Sprintf("interval=%s", interval), func(b *testing.B) {
			suppressLogs(b)
			host := newBenchHost(b, newSlowWALStore(walLatency), flushSize, interval)
			ctx := context.Background()
			warmup(b, host)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := host.Send(ctx, benchPartition, kvReq{Op: "write"}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")

			b.StopTimer()
			host.EvictAll(ctx) //nolint:errcheck
		})
	}
}
