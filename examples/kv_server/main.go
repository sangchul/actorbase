// examples/kv_server는 actorbase Partition Server를 KV Actor로 실행하는 예시다.
//
// ps 패키지가 제공하는 플랫폼 위에 사용자 정의 Actor(kvActor)를 올려서
// 인메모리 key-value 저장소를 구동한다.
//
// 실제 배포 시에는 이 파일을 참고하여 원하는 Actor 구현으로 교체한다.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/oomymy/actorbase/adapter/fs"
	adapterjson "github.com/oomymy/actorbase/adapter/json"
	"github.com/oomymy/actorbase/provider"
	"github.com/oomymy/actorbase/ps"
)

// ── KV Actor 타입 정의 ────────────────────────────────────────────────────────

// KVRequest는 KV Actor의 요청 타입이다.
type KVRequest struct {
	Op    string `json:"op"`    // "get", "set", "del"
	Key   string `json:"key"`
	Value []byte `json:"value"` // "set" 시에만 사용
}

// KVResponse는 KV Actor의 응답 타입이다.
type KVResponse struct {
	Value []byte `json:"value"`
	Found bool   `json:"found"`
}

// walOp는 WAL에 기록되는 변경 연산이다.
type walOp struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// kvActor는 파티션이 담당하는 키 범위의 데이터를 인메모리 map으로 관리한다.
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

func (a *kvActor) Snapshot() ([]byte, error) {
	return json.Marshal(a.data)
}

func (a *kvActor) Restore(data []byte) error {
	return json.Unmarshal(data, &a.data)
}

func (a *kvActor) Split(splitKey string) ([]byte, error) {
	upper := make(map[string][]byte)
	for k, v := range a.data {
		if k >= splitKey {
			upper[k] = v
			delete(a.data, k)
		}
	}
	return json.Marshal(upper)
}

// ── main ───────────────────────────────────────────────────────────────────────

func main() {
	nodeID := flag.String("node-id", "", "Unique node ID (default: hostname)")
	addr := flag.String("addr", ":7001", "gRPC listen address")
	etcdAddrs := flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
	walDir := flag.String("wal-dir", "/tmp/actorbase-ps/wal", "WAL directory (node-local)")
	checkpointDir := flag.String("checkpoint-dir", "/tmp/actorbase-ps/checkpoint", "Checkpoint directory (shared across PS nodes)")
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

	factory := func(_ string) provider.Actor[KVRequest, KVResponse] {
		return &kvActor{data: make(map[string][]byte)}
	}

	srv, err := ps.NewServer(ps.Config[KVRequest, KVResponse]{
		NodeID:          *nodeID,
		Addr:            *addr,
		EtcdEndpoints:   strings.Split(*etcdAddrs, ","),
		Factory:         factory,
		Codec:           adapterjson.New(),
		WALStore:        walStore,
		CheckpointStore: cpStore,
	})
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
