// Package s3common defines the shared request/response types for the S3 metadata example.
// Both the s3/server and s3/client import this package to ensure type compatibility.
package s3common

import "time"

// BucketRequest is a bucket metadata request.
type BucketRequest struct {
	Op       string `json:"op"`        // "create", "get", "delete", "list"
	Name     string `json:"name"`      // bucket name (= routing key)
	Region   string `json:"region"`    // used only for "create"
	StartKey string `json:"start_key"` // used for "list" (inclusive)
	EndKey   string `json:"end_key"`   // used for "list" (exclusive, ""=unbounded)
}

// BucketItem is a single item in a list result.
type BucketItem struct {
	Name      string    `json:"name"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
}

// BucketResponse is a bucket metadata response.
type BucketResponse struct {
	Name      string       `json:"name"`
	Region    string       `json:"region"`
	CreatedAt time.Time    `json:"created_at"`
	Found     bool         `json:"found"`
	Items     []BucketItem `json:"items"` // "list" results
}

// ObjectRequest is an object metadata request.
// The routing key is in the form "{bucket}/{key}".
type ObjectRequest struct {
	Op           string `json:"op"`            // "put", "get", "delete", "list"
	Bucket       string `json:"bucket"`        // bucket name
	Key          string `json:"key"`           // object key
	Size         int64  `json:"size"`          // used only for "put" (bytes)
	ETag         string `json:"etag"`          // used only for "put"
	StorageClass string `json:"storage_class"` // used only for "put" (STANDARD, etc.)
	StartKey     string `json:"start_key"`     // used for "list" (inclusive)
	EndKey       string `json:"end_key"`       // used for "list" (exclusive, ""=unbounded)
}

// ObjectItem is a single item in a list result.
type ObjectItem struct {
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
	LastModified time.Time `json:"last_modified"`
}

// ObjectResponse is an object metadata response.
type ObjectResponse struct {
	Bucket       string       `json:"bucket"`
	Key          string       `json:"key"`
	Size         int64        `json:"size"`
	ETag         string       `json:"etag"`
	StorageClass string       `json:"storage_class"`
	LastModified time.Time    `json:"last_modified"`
	Found        bool         `json:"found"`
	Items        []ObjectItem `json:"items"` // "list" results
}
