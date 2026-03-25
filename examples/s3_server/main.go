// examples/s3_server is an example of running an actorbase Partition Server as an S3 metadata server.
//
// It registers two actor types — bucketActor and objectActor — on a single PS to manage
// S3 bucket and object metadata using the actorbase cluster.
//
// Usage:
//
//	s3_server -node-id ps-1 -addr :8001 -etcd localhost:2379 \
//	          -wal-dir /tmp/s3/wal -checkpoint-dir /tmp/s3/checkpoint
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sangchul/actorbase/adapter/fs"
	adapterjson "github.com/sangchul/actorbase/adapter/json"
	"github.com/sangchul/actorbase/provider"
	"github.com/sangchul/actorbase/ps"
)

func main() {
	nodeID := flag.String("node-id", "", "Unique node ID (default: hostname)")
	addr := flag.String("addr", ":8001", "gRPC listen address")
	etcdAddrs := flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
	walDir := flag.String("wal-dir", "/tmp/actorbase-s3/wal", "WAL directory (shared across PS nodes)")
	checkpointDir := flag.String("checkpoint-dir", "/tmp/actorbase-s3/checkpoint", "Checkpoint directory (shared across PS nodes)")
	flag.Parse()

	if *nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			slog.Error("failed to get hostname", "err", err)
			os.Exit(1)
		}
		*nodeID = hostname
	}

	walStore, err := fs.NewWALStore(*walDir)
	if err != nil {
		slog.Error("failed to create WAL store", "err", err)
		os.Exit(1)
	}

	cpStore, err := fs.NewCheckpointStore(*checkpointDir)
	if err != nil {
		slog.Error("failed to create checkpoint store", "err", err)
		os.Exit(1)
	}

	builder := ps.NewServerBuilder(ps.BaseConfig{
		NodeID:        *nodeID,
		Addr:          *addr,
		EtcdEndpoints: strings.Split(*etcdAddrs, ","),
	})

	if err := ps.Register(builder, ps.TypeConfig[BucketRequest, BucketResponse]{
		TypeID: "bucket",
		Factory: func(_ string) provider.Actor[BucketRequest, BucketResponse] {
			return &bucketActor{buckets: make(map[string]bucketMeta)}
		},
		Codec:           adapterjson.New(),
		WALStore:        walStore,
		CheckpointStore: cpStore,
	}); err != nil {
		slog.Error("failed to register bucket actor type", "err", err)
		os.Exit(1)
	}

	if err := ps.Register(builder, ps.TypeConfig[ObjectRequest, ObjectResponse]{
		TypeID: "object",
		Factory: func(_ string) provider.Actor[ObjectRequest, ObjectResponse] {
			return &objectActor{objects: make(map[string]objectMeta), accessCt: make(map[string]int64)}
		},
		Codec:           adapterjson.New(),
		WALStore:        walStore,
		CheckpointStore: cpStore,
	}); err != nil {
		slog.Error("failed to register object actor type", "err", err)
		os.Exit(1)
	}

	srv, err := builder.Build()
	if err != nil {
		slog.Error("failed to create PS", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting S3 metadata PS",
		"node-id", *nodeID, "addr", *addr,
		"wal-dir", *walDir, "checkpoint-dir", *checkpointDir)
	if err := srv.Start(ctx); err != nil {
		slog.Error("PS stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("PS stopped")
}
