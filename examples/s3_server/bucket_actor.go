package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sangchul/actorbase/provider"
)

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
