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

// suppressLogs는 벤치마크 실행 중 slog 출력을 /dev/null로 보낸다.
// 활성화/비활성화 로그가 벤치마크 결과에 노이즈를 추가하기 때문이다.
func suppressLogs(b *testing.B) {
	b.Helper()
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(orig) })
}

// ── benchmark용 Actor ─────────────────────────────────────────────────────────
//
// nopActor는 actor 자체의 비즈니스 로직 overhead를 제거하여
// engine(mailbox + WAL group commit)의 순수 처리 비용만 측정한다.

type nopActor struct{}

func newNopActor(_ string) provider.Actor[kvReq, kvResp] { return &nopActor{} }

func (a *nopActor) Receive(_ provider.Context, req kvReq) (kvResp, []byte, error) {
	if req.Op == "write" {
		// non-nil walEntry → WAL group commit 경로
		return kvResp{Value: "ok"}, []byte("w"), nil
	}
	// nil walEntry → WAL 없이 즉시 응답 경로
	return kvResp{Value: "ok"}, nil, nil
}
func (a *nopActor) Replay(_ []byte) error          { return nil }
func (a *nopActor) Snapshot() ([]byte, error)      { return []byte("{}"), nil }
func (a *nopActor) Restore(_ []byte) error         { return nil }
func (a *nopActor) Split(_ string) ([]byte, error) { return []byte("{}"), nil }

// ── benchmark용 WAL Store ─────────────────────────────────────────────────────
//
// slowWALStore는 AppendBatch 1회(= flush 1회)당 일정 시간을 지연시켜
// SSD fsync 지연을 시뮬레이션한다.
//
// 핵심: 지연은 entry당이 아니라 배치당 부과된다.
// 동시 발신자가 N명이면 하나의 배치에 최대 N개의 entry가 묶이므로
// 유효 처리량은 concurrency가 올라갈수록 N배에 가깝게 증가한다.
// 이것이 group commit의 효과다.

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

// ── 헬퍼 ─────────────────────────────────────────────────────────────────────

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
	// actor 활성화 (WAL replay + checkpoint 로드)를 측정 시작 전에 완료
	if _, err := host.Send(ctx, benchPartition, kvReq{Op: "read"}); err != nil {
		b.Fatalf("warmup: %v", err)
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkActorHost_Read는 WAL 없는 읽기 처리량을 측정한다.
//
// walEntry=nil 경로: actor.Receive 완료 즉시 caller에게 응답.
// 이 수치가 engine의 처리 속도 상한이다.
// (mailbox channel send/recv + goroutine context switch 비용만 포함)
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

// BenchmarkActorHost_Write_MemWAL은 인메모리 WAL을 사용한 쓰기 처리량을 측정한다.
//
// group commit 경로(actor → flusher → reply)를 타지만 WAL IO 자체가 메모리이므로
// 지연이 거의 없다. Read 대비 감소분이 group commit 경로 자체의 overhead다.
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

// BenchmarkActorHost_Write_SlowWAL은 디스크 fsync 지연(500µs/batch)을 시뮬레이션하고
// 동시 발신자 수(concurrency)에 따른 처리량 변화를 측정한다.
//
// 핵심 비교:
//   - concurrency=1 (순차):  WAL 지연이 직접 노출 → ~1/latency ops/s
//   - concurrency=N (병렬):  N개의 요청이 한 배치에 묶임 → ~N/latency ops/s
//
// 이 수치가 group commit이 단일 스레드 actor의 쓰기 처리량을 어떻게 확장하는지 보여준다.
// actor goroutine은 WAL 완료를 기다리지 않고 즉시 다음 메시지를 처리하므로,
// WAL 지연은 throughput이 아니라 latency에만 영향을 준다.
func BenchmarkActorHost_Write_SlowWAL(b *testing.B) {
	const (
		walLatency    = 500 * time.Microsecond // 일반 SSD fsync 수준
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

// BenchmarkActorHost_FlushInterval은 FlushInterval 설정에 따른 쓰기 처리량을 비교한다.
//
// FlushInterval이 작을수록:
//   - latency 감소 (caller가 더 빨리 응답받음)
//   - batch size 감소 → WAL IO 횟수 증가 → throughput 감소
//
// FlushInterval이 클수록:
//   - throughput 증가 (더 많은 entry가 한 배치에 묶임)
//   - latency 증가 (caller 대기 시간 증가)
//
// 이 수치로 배포 환경의 디스크 특성에 맞는 FlushInterval을 선택할 수 있다.
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
