// examples/kv_longrun runs long-duration load tests and consistency verification against an actorbase cluster.
//
// Run modes:
//   - Load mode (default): workers repeatedly perform set/del/get and record successful operations in the ledger.
//     The ledger is periodically flushed to a file.
//   - Verify mode (-verify): loads the ledger, queries all keys from the server, and checks for consistency.
//
// Usage examples:
//
//	# 8-minute load test + ledger recording
//	kv_longrun -pm localhost:8000 -duration 8m -workers 20
//
//	# Verify after load completes
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

// KVRequest / KVResponse must match the types used by kv_server.
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

	// stopCh: channel used to signal workers to stop starting new requests.
	// Kept separate from ctx so that in-flight requests can complete fully via ctx before flushing.
	// This prevents a race condition where a request committed by the server just before the deadline
	// gets treated as a failure on the client side due to ctx cancellation, causing it to be missed in the ledger.
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

	// Wait until duration elapses or an external interrupt is received
	if duration > 0 {
		select {
		case <-time.After(duration):
		case <-ctx.Done():
		}
	} else {
		// duration=0: run indefinitely until interrupted
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
	// Signal workers to stop starting new requests; each worker finishes its current in-flight request before exiting
	close(stopCh)

	// Wait for all workers' in-flight requests to complete
	wg.Wait()

	// Flush after all workers have updated the ledger
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
