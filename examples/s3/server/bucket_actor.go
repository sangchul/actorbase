package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	s3common "github.com/sangchul/actorbase/examples/s3/common"
	"github.com/sangchul/actorbase/provider"
)

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

func (a *bucketActor) Receive(_ provider.Context, req s3common.BucketRequest) (s3common.BucketResponse, []byte, error) {
	switch req.Op {
	case "create":
		meta := bucketMeta{Region: req.Region, CreatedAt: time.Now().UTC()}
		a.buckets[req.Name] = meta
		entry, _ := json.Marshal(bucketWALOp{Op: "create", Name: req.Name, Meta: meta})
		return s3common.BucketResponse{Name: req.Name, Region: meta.Region, CreatedAt: meta.CreatedAt, Found: true}, entry, nil

	case "get":
		meta, ok := a.buckets[req.Name]
		if !ok {
			return s3common.BucketResponse{Found: false}, nil, nil
		}
		return s3common.BucketResponse{Name: req.Name, Region: meta.Region, CreatedAt: meta.CreatedAt, Found: true}, nil, nil

	case "delete":
		delete(a.buckets, req.Name)
		entry, _ := json.Marshal(bucketWALOp{Op: "delete", Name: req.Name})
		return s3common.BucketResponse{Found: true}, entry, nil

	case "list":
		var items []s3common.BucketItem
		for name, meta := range a.buckets {
			if name >= req.StartKey && (req.EndKey == "" || name < req.EndKey) {
				items = append(items, s3common.BucketItem{Name: name, Region: meta.Region, CreatedAt: meta.CreatedAt})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
		return s3common.BucketResponse{Items: items}, nil, nil

	default:
		return s3common.BucketResponse{}, nil, fmt.Errorf("unknown bucket op: %s", req.Op)
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

func (a *bucketActor) Export(splitKey string) ([]byte, error) {
	if splitKey == "" {
		return json.Marshal(a.buckets)
	}
	upper := make(map[string]bucketMeta)
	for k, v := range a.buckets {
		if k >= splitKey {
			upper[k] = v
			delete(a.buckets, k)
		}
	}
	return json.Marshal(upper)
}

func (a *bucketActor) Import(data []byte) error {
	var incoming map[string]bucketMeta
	if err := json.Unmarshal(data, &incoming); err != nil {
		return err
	}
	for k, v := range incoming {
		a.buckets[k] = v
	}
	return nil
}

func (a *bucketActor) KeyCount() int64 { return int64(len(a.buckets)) }
