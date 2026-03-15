package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sangchul/actorbase/provider"
)

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
