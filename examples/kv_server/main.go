// examples/kv_server is an example of running an actorbase Partition Server with a KV Actor.
//
// It mounts a user-defined Actor (kvActor) on top of the platform provided by the ps package
// to run an in-memory key-value store.
//
// For actual deployments, refer to this file and replace with your own Actor implementation.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	goredis "github.com/redis/go-redis/v9"

	"github.com/sangchul/actorbase/adapter/fs"
	adapterjson "github.com/sangchul/actorbase/adapter/json"
	adapterredis "github.com/sangchul/actorbase/adapter/redis"
	adapters3 "github.com/sangchul/actorbase/adapter/s3"
	"github.com/sangchul/actorbase/provider"
	"github.com/sangchul/actorbase/ps"
)

// ── KV Actor type definitions ─────────────────────────────────────────────────

// KVRequest is the request type for the KV Actor.
type KVRequest struct {
	Op       string `json:"op"`        // "get", "set", "del", "scan"
	Key      string `json:"key"`       // used for "get", "set", "del"
	Value    []byte `json:"value"`     // used only for "set"
	StartKey string `json:"start_key"` // used for "scan" (inclusive)
	EndKey   string `json:"end_key"`   // used for "scan" (exclusive, ""=unbounded)
}

// KVItem is a single item in a scan result.
type KVItem struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// KVResponse is the response type for the KV Actor.
type KVResponse struct {
	Value []byte   `json:"value"`
	Found bool     `json:"found"`
	Items []KVItem `json:"items"` // "scan" results
}

// walOp is a mutation operation recorded in the WAL.
type walOp struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// kvActor manages the data for the key range owned by the partition using an in-memory map.
type kvActor struct {
	data map[string][]byte
}

func (a *kvActor) Receive(_ provider.Context, req KVRequest) (KVResponse, []byte, error) {
	switch req.Op {
	case "get":
		v, ok := a.data[req.Key]
		return KVResponse{Value: v, Found: ok}, nil, nil
	case "set":
		a.data[req.Key] = req.Value
		entry, _ := json.Marshal(walOp{Op: "set", Key: req.Key, Value: req.Value})
		return KVResponse{}, entry, nil
	case "del":
		delete(a.data, req.Key)
		entry, _ := json.Marshal(walOp{Op: "del", Key: req.Key})
		return KVResponse{}, entry, nil
	case "scan":
		var items []KVItem
		for k, v := range a.data {
			if k >= req.StartKey && (req.EndKey == "" || k < req.EndKey) {
				items = append(items, KVItem{Key: k, Value: v})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
		return KVResponse{Items: items}, nil, nil
	default:
		return KVResponse{}, nil, fmt.Errorf("unknown op: %s", req.Op)
	}
}

func (a *kvActor) Replay(entry []byte) error {
	var op walOp
	if err := json.Unmarshal(entry, &op); err != nil {
		return err
	}
	switch op.Op {
	case "set":
		a.data[op.Key] = op.Value
	case "del":
		delete(a.data, op.Key)
	}
	return nil
}

func (a *kvActor) Export(splitKey string) ([]byte, error) {
	if splitKey == "" {
		return json.Marshal(a.data)
	}
	upper := make(map[string][]byte)
	for k, v := range a.data {
		if k >= splitKey {
			upper[k] = v
			delete(a.data, k)
		}
	}
	return json.Marshal(upper)
}

func (a *kvActor) Import(data []byte) error {
	var incoming map[string][]byte
	if err := json.Unmarshal(data, &incoming); err != nil {
		return err
	}
	for k, v := range incoming {
		a.data[k] = v
	}
	return nil
}

// KeyCount implements provider.Countable. Returns the number of keys currently held.
func (a *kvActor) KeyCount() int64 {
	return int64(len(a.data))
}

// ── Counter Actor type definitions ───────────────────────────────────────────

// CounterRequest is the request type for the Counter Actor.
type CounterRequest struct {
	Op  string `json:"op"`  // "inc", "dec", "get", "reset"
	Key string `json:"key"`
	By  int64  `json:"by"` // amount to inc/dec (treated as 1 if 0)
}

// CounterResponse is the response type for the Counter Actor.
type CounterResponse struct {
	Value int64 `json:"value"`
}

// counterWALOp is a counter mutation operation recorded in the WAL.
type counterWALOp struct {
	Op  string `json:"op"`
	Key string `json:"key"`
	By  int64  `json:"by"`
}

// counterActor manages the counters for the key range owned by the partition using an in-memory map.
type counterActor struct {
	data map[string]int64
}

func (a *counterActor) Receive(_ provider.Context, req CounterRequest) (CounterResponse, []byte, error) {
	switch req.Op {
	case "get":
		return CounterResponse{Value: a.data[req.Key]}, nil, nil
	case "inc":
		by := req.By
		if by == 0 {
			by = 1
		}
		a.data[req.Key] += by
		entry, _ := json.Marshal(counterWALOp{Op: "inc", Key: req.Key, By: by})
		return CounterResponse{Value: a.data[req.Key]}, entry, nil
	case "dec":
		by := req.By
		if by == 0 {
			by = 1
		}
		a.data[req.Key] -= by
		entry, _ := json.Marshal(counterWALOp{Op: "dec", Key: req.Key, By: by})
		return CounterResponse{Value: a.data[req.Key]}, entry, nil
	case "reset":
		a.data[req.Key] = 0
		entry, _ := json.Marshal(counterWALOp{Op: "reset", Key: req.Key})
		return CounterResponse{}, entry, nil
	default:
		return CounterResponse{}, nil, fmt.Errorf("unknown op: %s", req.Op)
	}
}

func (a *counterActor) Replay(entry []byte) error {
	var op counterWALOp
	if err := json.Unmarshal(entry, &op); err != nil {
		return err
	}
	switch op.Op {
	case "inc":
		a.data[op.Key] += op.By
	case "dec":
		a.data[op.Key] -= op.By
	case "reset":
		a.data[op.Key] = 0
	}
	return nil
}

func (a *counterActor) Export(splitKey string) ([]byte, error) {
	if splitKey == "" {
		return json.Marshal(a.data)
	}
	upper := make(map[string]int64)
	for k, v := range a.data {
		if k >= splitKey {
			upper[k] = v
			delete(a.data, k)
		}
	}
	return json.Marshal(upper)
}

func (a *counterActor) Import(data []byte) error {
	var incoming map[string]int64
	if err := json.Unmarshal(data, &incoming); err != nil {
		return err
	}
	for k, v := range incoming {
		a.data[k] = v
	}
	return nil
}

// KeyCount implements provider.Countable.
func (a *counterActor) KeyCount() int64 {
	return int64(len(a.data))
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	nodeID := flag.String("node-id", "", "Unique node ID (default: hostname)")
	addr := flag.String("addr", ":7001", "gRPC listen address")
	etcdAddrs := flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
	walDir := flag.String("wal-dir", "/tmp/actorbase/wal", "WAL directory (shared across PS nodes, partitioned by partition ID)")
	walBackend := flag.String("wal-backend", "fs", "WAL backend: fs | redis")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address (used when -wal-backend=redis)")
	redisPrefix := flag.String("redis-prefix", "wal", "Redis key prefix (used when -wal-backend=redis)")
	checkpointDir := flag.String("checkpoint-dir", "/tmp/actorbase/checkpoint", "Checkpoint directory (shared across PS nodes, used when -checkpoint-backend=fs)")
	checkpointBackend := flag.String("checkpoint-backend", "fs", "Checkpoint backend: fs | s3")
	s3Bucket := flag.String("s3-bucket", "", "S3 bucket name (used when -checkpoint-backend=s3)")
	s3Prefix := flag.String("s3-prefix", "checkpoint", "S3 object key prefix (used when -checkpoint-backend=s3)")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint override (e.g. http://localhost:9000, empty=AWS)")
	s3Region := flag.String("s3-region", "us-east-1", "S3 region (used when -checkpoint-backend=s3)")
	idleTimeout := flag.Duration("idle-timeout", 5*time.Minute, "Actor idle timeout before eviction")
	evictInterval := flag.Duration("evict-interval", time.Minute, "Eviction scheduler check interval")
	drainTimeout := flag.Duration("drain-timeout", 60*time.Second, "Maximum time to wait for partition drain on graceful shutdown")
	actorTypes := flag.String("actor-types", "kv", "Comma-separated list of actor types to enable (kv, counter)")
	flag.Parse()

	if *nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			slog.Error("failed to get hostname", "err", err)
			os.Exit(1)
		}
		*nodeID = hostname
	}

	var walStore provider.WALStore
	switch *walBackend {
	case "redis":
		rdb := goredis.NewClient(&goredis.Options{Addr: *redisAddr})
		walStore = adapterredis.NewWALStore(rdb, *redisPrefix)
		slog.Info("using Redis WAL store", "addr", *redisAddr, "prefix", *redisPrefix)
	case "fs":
		var err error
		walStore, err = fs.NewWALStore(*walDir)
		if err != nil {
			slog.Error("failed to create WAL store", "err", err)
			os.Exit(1)
		}
	default:
		slog.Error("unknown -wal-backend value", "value", *walBackend)
		os.Exit(1)
	}

	var cpStore provider.CheckpointStore
	switch *checkpointBackend {
	case "s3":
		if *s3Bucket == "" {
			slog.Error("-s3-bucket is required when -checkpoint-backend=s3")
			os.Exit(1)
		}
		opts := []func(*awsconfig.LoadOptions) error{
			awsconfig.WithRegion(*s3Region),
		}
		if *s3Endpoint != "" {
			opts = append(opts,
				awsconfig.WithBaseEndpoint(*s3Endpoint),
				awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					os.Getenv("AWS_ACCESS_KEY_ID"),
					os.Getenv("AWS_SECRET_ACCESS_KEY"),
					"",
				)),
			)
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
		if err != nil {
			slog.Error("failed to load AWS config", "err", err)
			os.Exit(1)
		}
		s3Client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
			if *s3Endpoint != "" {
				o.UsePathStyle = true
			}
		})
		cpStore = adapters3.NewCheckpointStore(s3Client, *s3Bucket, *s3Prefix)
		slog.Info("using S3 checkpoint store", "bucket", *s3Bucket, "prefix", *s3Prefix)
	case "fs":
		var err error
		cpStore, err = fs.NewCheckpointStore(*checkpointDir)
		if err != nil {
			slog.Error("failed to create checkpoint store", "err", err)
			os.Exit(1)
		}
	default:
		slog.Error("unknown -checkpoint-backend value", "value", *checkpointBackend)
		os.Exit(1)
	}

	builder := ps.NewServerBuilder(ps.BaseConfig{
		NodeID:        *nodeID,
		Addr:          *addr,
		EtcdEndpoints: strings.Split(*etcdAddrs, ","),
		IdleTimeout:   *idleTimeout,
		EvictInterval: *evictInterval,
		DrainTimeout:  *drainTimeout,
	})

	enabledTypes := make(map[string]bool)
	for _, t := range strings.Split(*actorTypes, ",") {
		enabledTypes[strings.TrimSpace(t)] = true
	}

	if enabledTypes["kv"] {
		kvFactory := func(_ string) provider.Actor[KVRequest, KVResponse] {
			return &kvActor{data: make(map[string][]byte)}
		}
		if err := ps.Register(builder, ps.TypeConfig[KVRequest, KVResponse]{
			TypeID:          "kv",
			Factory:         kvFactory,
			Codec:           adapterjson.New(),
			WALStore:        walStore,
			CheckpointStore: cpStore,
		}); err != nil {
			slog.Error("failed to register kv actor type", "err", err)
			os.Exit(1)
		}
	}

	if enabledTypes["counter"] {
		counterFactory := func(_ string) provider.Actor[CounterRequest, CounterResponse] {
			return &counterActor{data: make(map[string]int64)}
		}
		if err := ps.Register(builder, ps.TypeConfig[CounterRequest, CounterResponse]{
			TypeID:          "counter",
			Factory:         counterFactory,
			Codec:           adapterjson.New(),
			WALStore:        walStore,
			CheckpointStore: cpStore,
		}); err != nil {
			slog.Error("failed to register counter actor type", "err", err)
			os.Exit(1)
		}
	}
	srv, err := builder.Build()
	if err != nil {
		slog.Error("failed to create PS", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting PS", "node-id", *nodeID, "addr", *addr, "wal-dir", *walDir, "checkpoint-dir", *checkpointDir)
	if err := srv.Start(ctx); err != nil {
		slog.Error("PS stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("PS stopped")
}
