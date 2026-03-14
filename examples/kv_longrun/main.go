// examples/kv_longrun은 actorbase 클러스터를 대상으로 장기 부하 테스트와 일관성 검증을 수행한다.
//
// 실행 모드:
//   - 부하 모드 (기본): worker가 set/del/get을 반복하고, 성공한 연산을 ledger에 기록한다.
//     ledger는 주기적으로 파일에 flush된다.
//   - 검증 모드 (-verify): ledger를 불러와 서버의 모든 키를 조회하고 일치 여부를 확인한다.
//
// 사용 예:
//
//	# 8분 부하 + 정답지 기록
//	kv_longrun -pm localhost:8000 -duration 8m -workers 20
//
//	# 부하 종료 후 검증
//	kv_longrun -pm localhost:8000 -verify
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	adapterjson "github.com/sangchul/actorbase/adapter/json"
	"github.com/sangchul/actorbase/sdk"
)

// KVRequest / KVResponse는 kv_server와 동일하다.
type KVRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type KVResponse struct {
	Value []byte `json:"value"`
	Found bool   `json:"found"`
}

func main() {
	pmAddr := flag.String("pm", "localhost:8000", "PM gRPC address")
	duration := flag.Duration("duration", 8*time.Minute, "run duration (0 = run until interrupted)")
	numWorkers := flag.Int("workers", 20, "number of concurrent worker goroutines")
	numKeys := flag.Int("keys", 10000, "key space size (key:00000000 ~ key:9999999)")
	ledgerPath := flag.String("ledger", "/tmp/ab_ledger.json", "ledger file path")
	flushInterval := flag.Duration("flush-interval", 10*time.Second, "ledger flush interval")
	setRatio := flag.Float64("set-ratio", 0.6, "fraction of operations that are set (0.0~1.0)")
	delRatio := flag.Float64("del-ratio", 0.2, "fraction of operations that are del (rest = get)")
	doVerify := flag.Bool("verify", false, "verify mode: load ledger and check server state")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := sdk.NewClient(sdk.Config[KVRequest, KVResponse]{
		PMAddr:        *pmAddr,
		TypeID:        "kv",
		Codec:         adapterjson.New(),
		MaxRetries:    5,
		RetryInterval: 200 * time.Millisecond,
	})
	if err != nil {
		slog.Error("failed to create client", "err", err)
		os.Exit(1)
	}

	if err := client.Start(ctx); err != nil {
		slog.Error("client start failed", "err", err)
		os.Exit(1)
	}

	if *doVerify {
		runVerify(ctx, client, *ledgerPath)
		return
	}

	runLoad(ctx, client, *ledgerPath, *numWorkers, *numKeys, *flushInterval, *setRatio, *delRatio, *duration)
}

func runLoad(
	ctx context.Context,
	client *sdk.Client[KVRequest, KVResponse],
	ledgerPath string,
	numWorkers, numKeys int,
	flushInterval time.Duration,
	setRatio, delRatio float64,
	duration time.Duration,
) {
	ledger := NewLedger(ledgerPath)

	// stopCh: worker가 새 요청을 시작하지 않도록 신호하는 채널.
	// ctx와 분리하여 in-flight 요청이 ctx로 완전히 완료된 후 flush한다.
	// 이렇게 하면 deadline 직전에 서버에서 커밋된 요청이 ctx 취소로 인해
	// 클라이언트 측에서 실패 처리되어 ledger에 누락되는 race condition을 방지한다.
	stopCh := make(chan struct{})

	stats := &Stats{}
	wcfg := workerConfig{setRatio: setRatio, delRatio: delRatio}
	keysPerWorker := numKeys / numWorkers

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		low := i * keysPerWorker
		high := low + keysPerWorker
		if i == numWorkers-1 {
			high = numKeys
		}
		w := &worker{
			id:      i,
			keyLow:  low,
			keyHigh: high,
			client:  client,
			ledger:  ledger,
			cfg:     wcfg,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.run(ctx, stopCh, stats)
		}()
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	reportTicker := time.NewTicker(10 * time.Second)
	defer reportTicker.Stop()

	slog.Info("load started",
		"workers", numWorkers,
		"keys", numKeys,
		"set-ratio", setRatio,
		"del-ratio", delRatio,
		"duration", duration,
	)

	// duration 또는 외부 인터럽트까지 대기
	if duration > 0 {
		select {
		case <-time.After(duration):
		case <-ctx.Done():
		}
	} else {
		// duration=0: 인터럽트까지 무한 실행
		for {
			select {
			case <-ctx.Done():
				goto done
			case <-flushTicker.C:
				if err := ledger.Flush(); err != nil {
					slog.Error("ledger flush failed", "err", err)
				}
			case <-reportTicker.C:
				slog.Info("progress",
					"success", stats.success.Load(),
					"fail", stats.fail.Load(),
				)
			}
		}
	}

done:
	// 새 요청 중단 신호 → worker들이 현재 진행 중인 요청만 완료 후 종료
	close(stopCh)

	// 모든 worker의 in-flight 요청 완료 대기
	wg.Wait()

	// 모든 worker가 ledger를 업데이트한 후 flush
	if err := ledger.Flush(); err != nil {
		slog.Error("final ledger flush failed", "err", err)
	} else {
		slog.Info("ledger flushed", "path", ledgerPath)
	}
	slog.Info("load done",
		"success", stats.success.Load(),
		"fail", stats.fail.Load(),
	)
}

func runVerify(ctx context.Context, client *sdk.Client[KVRequest, KVResponse], ledgerPath string) {
	ledger, err := LoadLedger(ledgerPath)
	if err != nil {
		slog.Error("failed to load ledger", "err", err, "path", ledgerPath)
		os.Exit(1)
	}

	total := len(ledger.Snapshot())
	slog.Info("verifying", "keys", total, "ledger", ledgerPath)

	mismatches, err := verify(ctx, client, ledger)
	if err != nil {
		slog.Error("verification error", "err", err)
		os.Exit(1)
	}

	if len(mismatches) == 0 {
		slog.Info("PASS", "checked", total)
		return
	}

	slog.Error("FAIL", "mismatches", len(mismatches), "checked", total)
	for i, m := range mismatches {
		if i >= 20 {
			fmt.Printf("  ... and %d more\n", len(mismatches)-20)
			break
		}
		exp := "<deleted>"
		if m.Expected != nil {
			exp = *m.Expected
		}
		got := "<not found>"
		if m.Got != nil {
			got = *m.Got
		}
		fmt.Printf("  key=%-20s expected=%-30s got=%s\n", m.Key, exp, got)
	}
	os.Exit(1)
}
