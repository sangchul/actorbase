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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := sdk.NewClient(sdk.Config[KVRequest, KVResponse]{
		PMAddr: "localhost:8000",
		Codec:  adapterjson.New(),
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Start(ctx); err != nil {
		log.Fatalf("start failed: %v", err)
	}

	fmt.Println("=== migrate 후 checkpoint 복원 확인 (set 없이 get만) ===")
	// apple: ps-1에 있던 데이터
	mustGet(ctx, client, "apple")
	// mango: ps-1 → ps-2로 migrate된 데이터 (checkpoint 복원)
	mustGet(ctx, client, "mango")

	cancel()
}

func mustGet(ctx context.Context, c *sdk.Client[KVRequest, KVResponse], key string) {
	resp, err := c.Send(ctx, key, KVRequest{Op: "get", Key: key})
	if err != nil {
		log.Fatalf("get %s failed: %v", key, err)
	}
	fmt.Printf("get %-10s → found=%v value=%s\n", key, resp.Found, resp.Value)
}
