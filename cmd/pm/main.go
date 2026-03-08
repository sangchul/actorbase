// cmd/pm은 actorbase Partition Manager 실행 바이너리다.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/oomymy/actorbase/pm"
)

func main() {
	addr := flag.String("addr", ":7000", "gRPC listen address")
	etcdAddrs := flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
	actorTypes := flag.String("actor-types", "", "actor types to bootstrap (comma-separated, e.g. kv or bucket,object)")
	flag.Parse()

	if *actorTypes == "" {
		slog.Error("flag -actor-types is required (e.g. -actor-types kv)")
		os.Exit(1)
	}

	srv, err := pm.NewServer(pm.Config{
		ListenAddr:    *addr,
		EtcdEndpoints: strings.Split(*etcdAddrs, ","),
		ActorTypes:    strings.Split(*actorTypes, ","),
	})
	if err != nil {
		slog.Error("failed to create PM", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("starting PM", "addr", *addr, "actor_types", *actorTypes)
	if err := srv.Start(ctx); err != nil {
		slog.Error("PM stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("PM stopped")
}
