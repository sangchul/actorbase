// examples/s3_server는 actorbase Partition Server를 S3 메타데이터 서버로 실행하는 예시다.
//
// bucketActor와 objectActor 두 가지 actor type을 단일 PS에 등록하여
// S3의 bucket과 object 메타데이터를 actorbase 클러스터로 관리한다.
//
// 사용법:
//
//	s3_server -node-id ps-1 -addr :8001 -etcd localhost:2379 \
//	          -wal-dir /tmp/s3/wal -checkpoint-dir /tmp/s3/checkpoint
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"context"

	"github.com/oomymy/actorbase/adapter/fs"
	adapterjson "github.com/oomymy/actorbase/adapter/json"
	"github.com/oomymy/actorbase/provider"
	"github.com/oomymy/actorbase/ps"
)

// ── Bucket Actor ──────────────────────────────────────────────────────────────

// BucketRequest는 bucket 메타데이터 요청.
type BucketRequest struct {
	Op     string `json:"op"`     // "create", "get", "delete"
	Name   string `json:"name"`   // bucket name (= routing key)
	Region string `json:"region"` // "create" 시에만 사용
}

// BucketResponse는 bucket 메타데이터 응답.
type BucketResponse struct {
	Name      string    `json:"name"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
	Found     bool      `json:"found"`
}

type bucketMeta struct {
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
}

type bucketWALOp struct {
	Op   string     `json:"op"`
	Name string     `json:"name"`
	Meta bucketMeta `json:"meta,omitempty"`
}

type bucketActor struct {
	buckets map[string]bucketMeta // name → meta
}

func (a *bucketActor) Receive(_ provider.Context, req BucketRequest) (BucketResponse, []byte, error) {
	switch req.Op {
	case "create":
		meta := bucketMeta{Region: req.Region, CreatedAt: time.Now().UTC()}
		a.buckets[req.Name] = meta
		entry, _ := json.Marshal(bucketWALOp{Op: "create", Name: req.Name, Meta: meta})
		return BucketResponse{Name: req.Name, Region: meta.Region, CreatedAt: meta.CreatedAt, Found: true}, entry, nil

	case "get":
		meta, ok := a.buckets[req.Name]
		if !ok {
			return BucketResponse{Found: false}, nil, nil
		}
		return BucketResponse{Name: req.Name, Region: meta.Region, CreatedAt: meta.CreatedAt, Found: true}, nil, nil

	case "delete":
		delete(a.buckets, req.Name)
		entry, _ := json.Marshal(bucketWALOp{Op: "delete", Name: req.Name})
		return BucketResponse{Found: true}, entry, nil

	default:
		return BucketResponse{}, nil, fmt.Errorf("unknown bucket op: %s", req.Op)
	}
}

func (a *bucketActor) Replay(entry []byte) error {
	var op bucketWALOp
	if err := json.Unmarshal(entry, &op); err != nil {
		return err
	}
	switch op.Op {
	case "create":
		a.buckets[op.Name] = op.Meta
	case "delete":
		delete(a.buckets, op.Name)
	}
	return nil
}

func (a *bucketActor) Snapshot() ([]byte, error) {
	return json.Marshal(a.buckets)
}

func (a *bucketActor) Restore(data []byte) error {
	return json.Unmarshal(data, &a.buckets)
}

func (a *bucketActor) Split(splitKey string) ([]byte, error) {
	upper := make(map[string]bucketMeta)
	for k, v := range a.buckets {
		if k >= splitKey {
			upper[k] = v
			delete(a.buckets, k)
		}
	}
	return json.Marshal(upper)
}

func (a *bucketActor) KeyCount() int64 { return int64(len(a.buckets)) }

// ── Object Actor ──────────────────────────────────────────────────────────────

// ObjectRequest는 object 메타데이터 요청.
// routing key는 "{bucket}/{key}" 형태.
type ObjectRequest struct {
	Op           string `json:"op"`            // "put", "get", "delete"
	Bucket       string `json:"bucket"`        // bucket name
	Key          string `json:"key"`           // object key
	Size         int64  `json:"size"`          // "put" 시에만 사용 (bytes)
	ETag         string `json:"etag"`          // "put" 시에만 사용
	StorageClass string `json:"storage_class"` // "put" 시에만 사용 (STANDARD, etc.)
}

// ObjectResponse는 object 메타데이터 응답.
type ObjectResponse struct {
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
	LastModified time.Time `json:"last_modified"`
	Found        bool      `json:"found"`
}

type objectMeta struct {
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
	LastModified time.Time `json:"last_modified"`
}

type objectWALOp struct {
	Op     string     `json:"op"`
	ObjKey string     `json:"obj_key"` // "{bucket}/{key}"
	Meta   objectMeta `json:"meta,omitempty"`
}

type objectActor struct {
	objects map[string]objectMeta // "{bucket}/{key}" → meta
}

func objKey(bucket, key string) string {
	return bucket + "/" + key
}

func (a *objectActor) Receive(_ provider.Context, req ObjectRequest) (ObjectResponse, []byte, error) {
	k := objKey(req.Bucket, req.Key)
	switch req.Op {
	case "put":
		meta := objectMeta{
			Size:         req.Size,
			ETag:         req.ETag,
			StorageClass: req.StorageClass,
			LastModified: time.Now().UTC(),
		}
		a.objects[k] = meta
		entry, _ := json.Marshal(objectWALOp{Op: "put", ObjKey: k, Meta: meta})
		return ObjectResponse{
			Bucket: req.Bucket, Key: req.Key,
			Size: meta.Size, ETag: meta.ETag,
			StorageClass: meta.StorageClass, LastModified: meta.LastModified,
			Found: true,
		}, entry, nil

	case "get":
		meta, ok := a.objects[k]
		if !ok {
			return ObjectResponse{Found: false}, nil, nil
		}
		return ObjectResponse{
			Bucket: req.Bucket, Key: req.Key,
			Size: meta.Size, ETag: meta.ETag,
			StorageClass: meta.StorageClass, LastModified: meta.LastModified,
			Found: true,
		}, nil, nil

	case "delete":
		delete(a.objects, k)
		entry, _ := json.Marshal(objectWALOp{Op: "delete", ObjKey: k})
		return ObjectResponse{Found: true}, entry, nil

	default:
		return ObjectResponse{}, nil, fmt.Errorf("unknown object op: %s", req.Op)
	}
}

func (a *objectActor) Replay(entry []byte) error {
	var op objectWALOp
	if err := json.Unmarshal(entry, &op); err != nil {
		return err
	}
	switch op.Op {
	case "put":
		a.objects[op.ObjKey] = op.Meta
	case "delete":
		delete(a.objects, op.ObjKey)
	}
	return nil
}

func (a *objectActor) Snapshot() ([]byte, error) {
	return json.Marshal(a.objects)
}

func (a *objectActor) Restore(data []byte) error {
	return json.Unmarshal(data, &a.objects)
}

func (a *objectActor) Split(splitKey string) ([]byte, error) {
	upper := make(map[string]objectMeta)
	for k, v := range a.objects {
		if k >= splitKey {
			upper[k] = v
			delete(a.objects, k)
		}
	}
	return json.Marshal(upper)
}

func (a *objectActor) KeyCount() int64 { return int64(len(a.objects)) }

// ── main ──────────────────────────────────────────────────────────────────────

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

	// bucket actor 등록
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

	// object actor 등록
	if err := ps.Register(builder, ps.TypeConfig[ObjectRequest, ObjectResponse]{
		TypeID: "object",
		Factory: func(_ string) provider.Actor[ObjectRequest, ObjectResponse] {
			return &objectActor{objects: make(map[string]objectMeta)}
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
