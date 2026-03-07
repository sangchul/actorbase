// examples/kv_client는 actorbase SDK를 사용한 KV 클라이언트 예시다.
//
// kv_server와 함께 사용하며, key-value CRUD를 CLI로 실행할 수 있다.
// actorbase로 자체 분산 KV 저장소를 구축하는 사람이 SDK 사용법을 참고하는 데 목적이 있다.
//
// 사용법:
//
//	kv_client [-pm <addr>] <get|set|del> <key> [value]
//
// 예시:
//
//	kv_client set apple red
//	kv_client get apple
//	kv_client del apple
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	adapterjson "github.com/oomymy/actorbase/adapter/json"
	"github.com/oomymy/actorbase/sdk"
)

// KVRequest / KVResponse는 kv_server의 Actor가 처리하는 타입과 동일해야 한다.
// (Codec이 동일한 직렬화 형식을 사용하므로 구조체 레이아웃만 맞으면 된다.)

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
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: kv_client [-pm <addr>] <get|set|del> <key> [value]

Flags:
  -pm string   PM gRPC address (default: localhost:8000)

Commands:
  get <key>          Retrieve a value by key
  set <key> <value>  Store a key-value pair
  del <key>          Delete a key

`)
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	op := flag.Arg(0)
	key := flag.Arg(1)

	switch op {
	case "get", "del":
		// ok
	case "set":
		if flag.NArg() < 3 {
			fmt.Fprintln(os.Stderr, "usage: kv_client set <key> <value>")
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", op)
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := sdk.NewClient(sdk.Config[KVRequest, KVResponse]{
		PMAddr: *pmAddr,
		Codec:  adapterjson.New(),
	})
	if err != nil {
		slog.Error("failed to create client", "err", err)
		os.Exit(1)
	}

	if err := client.Start(ctx); err != nil {
		slog.Error("failed to start client", "err", err)
		os.Exit(1)
	}

	switch op {
	case "get":
		resp, err := client.Send(ctx, key, KVRequest{Op: "get", Key: key})
		if err != nil {
			slog.Error("get failed", "key", key, "err", err)
			os.Exit(1)
		}
		if !resp.Found {
			fmt.Fprintf(os.Stderr, "key %q not found\n", key)
			os.Exit(1)
		}
		fmt.Printf("%s\n", resp.Value)

	case "set":
		value := flag.Arg(2)
		_, err := client.Send(ctx, key, KVRequest{Op: "set", Key: key, Value: []byte(value)})
		if err != nil {
			slog.Error("set failed", "key", key, "err", err)
			os.Exit(1)
		}
		fmt.Println("ok")

	case "del":
		_, err := client.Send(ctx, key, KVRequest{Op: "del", Key: key})
		if err != nil {
			slog.Error("del failed", "key", key, "err", err)
			os.Exit(1)
		}
		fmt.Println("ok")
	}
}
