package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/sangchul/actorbase/provider"
)

// ObjectRequest는 object 메타데이터 요청.
// routing key는 "{bucket}/{key}" 형태.
type ObjectRequest struct {
	Op           string `json:"op"`            // "put", "get", "delete", "list"
	Bucket       string `json:"bucket"`        // bucket name
	Key          string `json:"key"`           // object key
	Size         int64  `json:"size"`          // "put" 시에만 사용 (bytes)
	ETag         string `json:"etag"`          // "put" 시에만 사용
	StorageClass string `json:"storage_class"` // "put" 시에만 사용 (STANDARD, etc.)
	StartKey     string `json:"start_key"`     // "list" 시 사용 (포함)
	EndKey       string `json:"end_key"`       // "list" 시 사용 (미포함, ""=무한대)
}

// ObjectItem은 list 결과 항목이다.
type ObjectItem struct {
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
	LastModified time.Time `json:"last_modified"`
}

// ObjectResponse는 object 메타데이터 응답.
type ObjectResponse struct {
	Bucket       string       `json:"bucket"`
	Key          string       `json:"key"`
	Size         int64        `json:"size"`
	ETag         string       `json:"etag"`
	StorageClass string       `json:"storage_class"`
	LastModified time.Time    `json:"last_modified"`
	Found        bool         `json:"found"`
	Items        []ObjectItem `json:"items"` // "list" 결과
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
	objects  map[string]objectMeta // "{bucket}/{key}" → meta
	accessCt map[string]int64      // routing key별 누적 접근 수 (hotspot 추적)
}

func objKey(bucket, key string) string {
	return bucket + "/" + key
}

func (a *objectActor) Receive(_ provider.Context, req ObjectRequest) (ObjectResponse, []byte, error) {
	k := objKey(req.Bucket, req.Key)
	a.accessCt[k]++ // mailbox goroutine 내에서만 호출되므로 별도 동기화 불필요
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

	case "list":
		var items []ObjectItem
		for objK, meta := range a.objects {
			if objK >= req.StartKey && (req.EndKey == "" || objK < req.EndKey) {
				// routing key는 "{bucket}/{key}" 형태 — bucket과 key 분리
				bucket, key := parseBucketKey(objK)
				items = append(items, ObjectItem{
					Bucket:       bucket,
					Key:          key,
					Size:         meta.Size,
					ETag:         meta.ETag,
					StorageClass: meta.StorageClass,
					LastModified: meta.LastModified,
				})
			}
		}
		sort.Slice(items, func(i, j int) bool {
			return objKey(items[i].Bucket, items[i].Key) < objKey(items[j].Bucket, items[j].Key)
		})
		return ObjectResponse{Items: items}, nil, nil

	default:
		return ObjectResponse{}, nil, fmt.Errorf("unknown object op: %s", req.Op)
	}
}

// parseBucketKey는 "{bucket}/{key}" routing key를 분리한다.
func parseBucketKey(rk string) (bucket, key string) {
	for i, c := range rk {
		if c == '/' {
			return rk[:i], rk[i+1:]
		}
	}
	return rk, ""
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

// SplitHint는 가장 많이 접근된 routing key를 split 위치로 제안한다.
// 해당 key 이상의 object들이 상위 파티션으로 이동하므로 hotspot이 분산된다.
// mailbox goroutine 내에서 호출되므로 accessCt 접근이 thread-safe하다.
func (a *objectActor) SplitHint() string {
	var hotKey string
	var maxCt int64
	for k, ct := range a.accessCt {
		if ct > maxCt {
			maxCt = ct
			hotKey = k
		}
	}
	return hotKey
}
