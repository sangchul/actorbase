// examples/kv_client is an example KV client using the actorbase SDK.
//
// Used together with kv_server, it allows key-value CRUD operations via CLI.
// It is intended as a reference for SDK usage when building a custom distributed KV store with actorbase.
//
// Usage:
//
//	kv_client [-pm <addr>] <get|set|del> <key> [value]
//
// Examples:
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
	"strings"
	"time"

	adapterjson "github.com/sangchul/actorbase/adapter/json"
	"github.com/sangchul/actorbase/sdk"
)

// KVRequest / KVResponse must match the types handled by the kv_server Actor.
// (Only the struct layout needs to match, since both sides use the same Codec serialization format.)

type KVRequest struct {
	Op       string `json:"op"`
	Key      string `json:"key"`
	Value    []byte `json:"value"`
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

type KVItem struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type KVResponse struct {
	Value []byte   `json:"value"`
	Found bool     `json:"found"`
	Items []KVItem `json:"items"`
}

func main() {
	pmAddr := flag.String("pm", "", "PM gRPC address (mutually exclusive with -etcd)")
	etcdAddrs := flag.String("etcd", "", "etcd endpoints for HA mode, comma-separated (mutually exclusive with -pm)")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: kv_client [-pm <addr>|-etcd <endpoints>] <get|set|del|scan> <key|start> [value|end]

Flags:
  -pm   string   PM gRPC address (default: localhost:8000)
  -etcd string   etcd endpoints for HA mode (comma-separated)

Commands:
  get <key>              Retrieve a value by key
  set <key> <value>      Store a key-value pair
  del <key>              Delete a key
  scan <start> [end]     List all keys in [start, end) across partitions

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
	case "scan":
		// ok — end key is optional (default: "")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", op)
		flag.Usage()
		os.Exit(1)
	}

	// -pm default: localhost:8000 when -etcd is not specified
	cfg := sdk.Config[KVRequest, KVResponse]{
		TypeID: "kv",
		Codec:  adapterjson.New(),
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := sdk.NewClient(cfg)
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

	case "scan":
		startKey := key
		endKey := ""
		if flag.NArg() >= 3 {
			endKey = flag.Arg(2)
		}
		req := KVRequest{Op: "scan", StartKey: startKey, EndKey: endKey}
		partResults, err := client.Scan(ctx, startKey, endKey, req)
		if err != nil {
			slog.Error("scan failed", "err", err)
			os.Exit(1)
		}
		total := 0
		for _, pr := range partResults {
			for _, item := range pr.Items {
				fmt.Printf("%s\t%s\n", item.Key, item.Value)
				total++
			}
		}
		if total == 0 {
			fmt.Println("(no keys found)")
		}
	}
}
