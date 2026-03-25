package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/sangchul/actorbase/provider"
)

// BucketRequest는 bucket 메타데이터 요청.
type BucketRequest struct {
	Op       string `json:"op"`        // "create", "get", "delete", "list"
	Name     string `json:"name"`      // bucket name (= routing key)
	Region   string `json:"region"`    // "create" 시에만 사용
	StartKey string `json:"start_key"` // "list" 시 사용 (포함)
	EndKey   string `json:"end_key"`   // "list" 시 사용 (미포함, ""=무한대)
}

// BucketItem은 list 결과 항목이다.
type BucketItem struct {
	Name      string    `json:"name"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
}

// BucketResponse는 bucket 메타데이터 응답.
type BucketResponse struct {
	Name      string       `json:"name"`
	Region    string       `json:"region"`
	CreatedAt time.Time    `json:"created_at"`
	Found     bool         `json:"found"`
	Items     []BucketItem `json:"items"` // "list" 결과
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

	case "list":
		var items []BucketItem
		for name, meta := range a.buckets {
			if name >= req.StartKey && (req.EndKey == "" || name < req.EndKey) {
				items = append(items, BucketItem{Name: name, Region: meta.Region, CreatedAt: meta.CreatedAt})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
		return BucketResponse{Items: items}, nil, nil

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
