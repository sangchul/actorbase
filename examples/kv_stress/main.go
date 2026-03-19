package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	adapterjson "github.com/sangchul/actorbase/adapter/json"
	"github.com/sangchul/actorbase/sdk"
)

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
	pmAddr := flag.String("pm", "", "PM gRPC address (mutually exclusive with -etcd)")
	etcdAddrs := flag.String("etcd", "", "etcd endpoints for HA mode, comma-separated (mutually exclusive with -pm)")
	duration := flag.Duration("duration", 60*time.Second, "Stress test duration")
	interval := flag.Duration("interval", 100*time.Millisecond, "Request interval")
	maxRetries := flag.Int("max-retries", 5, "Maximum retries per request")
	retryInterval := flag.Duration("retry-interval", 200*time.Millisecond, "Retry interval")
	flag.Parse()

	cfg := sdk.Config[KVRequest, KVResponse]{
		TypeID:        "kv",
		Codec:         adapterjson.New(),
		MaxRetries:    *maxRetries,
		RetryInterval: *retryInterval,
	}
	if *etcdAddrs != "" {
		cfg.EtcdEndpoints = strings.Split(*etcdAddrs, ",")
	} else {
		addr := *pmAddr
		if addr == "" {
			addr = "localhost:8000"
		}
		cfg.PMAddr = addr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := sdk.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Start(ctx); err != nil {
		log.Fatalf("start failed: %v", err)
	}

	success, fail := 0, 0
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	timer := time.NewTimer(*duration)
	defer timer.Stop()

	i := 0
	for {
		select {
		case <-timer.C:
			fmt.Printf("\n완료: success=%d fail=%d\n", success, fail)
			return
		case <-ticker.C:
			key := fmt.Sprintf("key:%06d", i%1000)
			_, err := client.Send(ctx, key, KVRequest{Op: "set", Key: key, Value: []byte("v")})
			if err != nil {
				fmt.Printf("fail [%s]: %v\n", key, err)
				fail++
			} else {
				success++
				if success%50 == 0 {
					fmt.Printf("success=%d fail=%d\n", success, fail)
				}
			}
			i++
		}
	}
}
