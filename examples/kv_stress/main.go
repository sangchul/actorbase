package main

import (
	"context"
	"fmt"
	"log"
	"time"

	adapterjson "github.com/oomymy/actorbase/adapter/json"
	"github.com/oomymy/actorbase/sdk"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := sdk.NewClient(sdk.Config[KVRequest, KVResponse]{
		PMAddr:        "localhost:8000",
		Codec:         adapterjson.New(),
		MaxRetries:    5,
		RetryInterval: 200 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Start(ctx); err != nil {
		log.Fatalf("start failed: %v", err)
	}

	success, fail := 0, 0
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(60 * time.Second)
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
