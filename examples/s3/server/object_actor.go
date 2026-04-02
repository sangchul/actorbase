package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	s3common "github.com/sangchul/actorbase/examples/s3/common"
	"github.com/sangchul/actorbase/provider"
)

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
	accessCt map[string]int64      // cumulative access count per routing key (for hotspot tracking)
}

func objKey(bucket, key string) string {
	return bucket + "/" + key
}

func (a *objectActor) Receive(_ provider.Context, req s3common.ObjectRequest) (s3common.ObjectResponse, []byte, error) {
	k := objKey(req.Bucket, req.Key)
	a.accessCt[k]++ // called only within the mailbox goroutine, so no separate synchronization needed
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
		return s3common.ObjectResponse{
			Bucket: req.Bucket, Key: req.Key,
			Size: meta.Size, ETag: meta.ETag,
			StorageClass: meta.StorageClass, LastModified: meta.LastModified,
			Found: true,
		}, entry, nil

	case "get":
		meta, ok := a.objects[k]
		if !ok {
			return s3common.ObjectResponse{Found: false}, nil, nil
		}
		return s3common.ObjectResponse{
			Bucket: req.Bucket, Key: req.Key,
			Size: meta.Size, ETag: meta.ETag,
			StorageClass: meta.StorageClass, LastModified: meta.LastModified,
			Found: true,
		}, nil, nil

	case "delete":
		delete(a.objects, k)
		entry, _ := json.Marshal(objectWALOp{Op: "delete", ObjKey: k})
		return s3common.ObjectResponse{Found: true}, entry, nil

	case "list":
		var items []s3common.ObjectItem
		for objK, meta := range a.objects {
			if objK >= req.StartKey && (req.EndKey == "" || objK < req.EndKey) {
				// routing key is in the form "{bucket}/{key}" — split into bucket and key
				bucket, key := parseBucketKey(objK)
				items = append(items, s3common.ObjectItem{
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
		return s3common.ObjectResponse{Items: items}, nil, nil

	default:
		return s3common.ObjectResponse{}, nil, fmt.Errorf("unknown object op: %s", req.Op)
	}
}

// parseBucketKey splits a "{bucket}/{key}" routing key into its bucket and key components.
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

func (a *objectActor) Export(splitKey string) ([]byte, error) {
	if splitKey == "" {
		return json.Marshal(a.objects)
	}
	upper := make(map[string]objectMeta)
	for k, v := range a.objects {
		if k >= splitKey {
			upper[k] = v
			delete(a.objects, k)
		}
	}
	return json.Marshal(upper)
}

func (a *objectActor) Import(data []byte) error {
	var incoming map[string]objectMeta
	if err := json.Unmarshal(data, &incoming); err != nil {
		return err
	}
	for k, v := range incoming {
		a.objects[k] = v
	}
	return nil
}

func (a *objectActor) KeyCount() int64 { return int64(len(a.objects)) }

// SplitHint suggests the most frequently accessed routing key as the split point.
// Objects at or above that key are moved to the upper partition, distributing the hotspot.
// Called within the mailbox goroutine, so access to accessCt is thread-safe.
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
