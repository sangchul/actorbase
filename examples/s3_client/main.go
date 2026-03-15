// examples/s3_client는 actorbase SDK를 사용한 S3 메타데이터 클라이언트 예시다.
//
// s3_server와 함께 사용하며, bucket/object CRUD를 CLI로 실행할 수 있다.
//
// 사용법:
//
//	s3_client [-pm <addr>] bucket <create|get|delete> <name> [region]
//	s3_client [-pm <addr>] object <put|get|delete> <bucket> <key> [size] [etag]
//
// 예시:
//
//	s3_client bucket create my-bucket us-east-1
//	s3_client bucket get my-bucket
//	s3_client object put my-bucket photo.jpg 1024 abc123
//	s3_client object get my-bucket photo.jpg
//	s3_client object delete my-bucket photo.jpg
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	adapterjson "github.com/sangchul/actorbase/adapter/json"
	"github.com/sangchul/actorbase/sdk"
)

// BucketRequest / BucketResponse — s3_server와 동일해야 한다.

type BucketRequest struct {
	Op       string `json:"op"`
	Name     string `json:"name"`
	Region   string `json:"region"`
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

type BucketItem struct {
	Name      string    `json:"name"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
}

type BucketResponse struct {
	Name      string       `json:"name"`
	Region    string       `json:"region"`
	CreatedAt time.Time    `json:"created_at"`
	Found     bool         `json:"found"`
	Items     []BucketItem `json:"items"`
}

// ObjectRequest / ObjectResponse — s3_server와 동일해야 한다.

type ObjectRequest struct {
	Op           string `json:"op"`
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	StorageClass string `json:"storage_class"`
	StartKey     string `json:"start_key"`
	EndKey       string `json:"end_key"`
}

type ObjectItem struct {
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
	LastModified time.Time `json:"last_modified"`
}

type ObjectResponse struct {
	Bucket       string       `json:"bucket"`
	Key          string       `json:"key"`
	Size         int64        `json:"size"`
	ETag         string       `json:"etag"`
	StorageClass string       `json:"storage_class"`
	LastModified time.Time    `json:"last_modified"`
	Found        bool         `json:"found"`
	Items        []ObjectItem `json:"items"`
}

func main() {
	pmAddr := flag.String("pm", "localhost:8000", "PM gRPC address")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: s3_client [-pm <addr>] <resource> <op> [args...]

Flags:
  -pm string   PM gRPC address (default: localhost:8000)

Resources:
  bucket create <name> <region>        Create a bucket
  bucket get <name>                    Get bucket metadata
  bucket delete <name>                 Delete a bucket
  bucket list [prefix]                 List all buckets (optionally filtered by prefix)

  object put <bucket> <key> <size> <etag>   Put object metadata
  object get <bucket> <key>                  Get object metadata
  object delete <bucket> <key>               Delete object metadata
  object list <bucket> [prefix]              List objects in a bucket (across partitions)

`)
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resource := flag.Arg(0)
	switch resource {
	case "bucket":
		runBucket(ctx, *pmAddr)
	case "object":
		runObject(ctx, *pmAddr)
	default:
		fmt.Fprintf(os.Stderr, "unknown resource: %s\n\n", resource)
		flag.Usage()
		os.Exit(1)
	}
}

func runBucket(ctx context.Context, pmAddr string) {
	client, err := sdk.NewClient(sdk.Config[BucketRequest, BucketResponse]{
		PMAddr: pmAddr,
		TypeID: "bucket",
		Codec:  adapterjson.New(),
	})
	if err != nil {
		slog.Error("failed to create bucket client", "err", err)
		os.Exit(1)
	}
	if err := client.Start(ctx); err != nil {
		slog.Error("failed to start bucket client", "err", err)
		os.Exit(1)
	}

	op := flag.Arg(1)
	switch op {
	case "create":
		if flag.NArg() < 4 {
			fmt.Fprintln(os.Stderr, "usage: s3_client bucket create <name> <region>")
			os.Exit(1)
		}
		name, region := flag.Arg(2), flag.Arg(3)
		resp, err := client.Send(ctx, name, BucketRequest{Op: "create", Name: name, Region: region})
		if err != nil {
			slog.Error("bucket create failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("created bucket: name=%s region=%s created_at=%s\n",
			resp.Name, resp.Region, resp.CreatedAt.Format(time.RFC3339))

	case "get":
		if flag.NArg() < 3 {
			fmt.Fprintln(os.Stderr, "usage: s3_client bucket get <name>")
			os.Exit(1)
		}
		name := flag.Arg(2)
		resp, err := client.Send(ctx, name, BucketRequest{Op: "get", Name: name})
		if err != nil {
			slog.Error("bucket get failed", "err", err)
			os.Exit(1)
		}
		if !resp.Found {
			fmt.Fprintf(os.Stderr, "bucket %q not found\n", name)
			os.Exit(1)
		}
		fmt.Printf("name=%s region=%s created_at=%s\n",
			resp.Name, resp.Region, resp.CreatedAt.Format(time.RFC3339))

	case "delete":
		if flag.NArg() < 3 {
			fmt.Fprintln(os.Stderr, "usage: s3_client bucket delete <name>")
			os.Exit(1)
		}
		name := flag.Arg(2)
		_, err := client.Send(ctx, name, BucketRequest{Op: "delete", Name: name})
		if err != nil {
			slog.Error("bucket delete failed", "err", err)
			os.Exit(1)
		}
		fmt.Println("deleted")

	case "list":
		prefix := ""
		if flag.NArg() >= 3 {
			prefix = flag.Arg(2)
		}
		endKey := ""
		if prefix != "" {
			// prefix 범위: [prefix, prefix의 마지막 바이트+1)
			endKey = prefixEnd(prefix)
		}
		req := BucketRequest{Op: "list", StartKey: prefix, EndKey: endKey}
		partResults, err := client.Scan(ctx, prefix, endKey, req)
		if err != nil {
			slog.Error("bucket list failed", "err", err)
			os.Exit(1)
		}
		total := 0
		for _, pr := range partResults {
			for _, item := range pr.Items {
				fmt.Printf("%-40s %-15s %s\n", item.Name, item.Region, item.CreatedAt.Format(time.RFC3339))
				total++
			}
		}
		if total == 0 {
			fmt.Println("(no buckets found)")
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown bucket op: %s\n", op)
		os.Exit(1)
	}
}

// prefixEnd는 prefix 범위의 상한 key를 반환한다.
// 예: "foo" → "fop" (마지막 바이트 +1). 0xFF이면 잘라낸다.
func prefixEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "" // 모든 바이트가 0xFF인 경우 — 상한 없음
}

func runObject(ctx context.Context, pmAddr string) {
	client, err := sdk.NewClient(sdk.Config[ObjectRequest, ObjectResponse]{
		PMAddr: pmAddr,
		TypeID: "object",
		Codec:  adapterjson.New(),
	})
	if err != nil {
		slog.Error("failed to create object client", "err", err)
		os.Exit(1)
	}
	if err := client.Start(ctx); err != nil {
		slog.Error("failed to start object client", "err", err)
		os.Exit(1)
	}

	op := flag.Arg(1)
	switch op {
	case "put":
		if flag.NArg() < 6 {
			fmt.Fprintln(os.Stderr, "usage: s3_client object put <bucket> <key> <size> <etag>")
			os.Exit(1)
		}
		bucket, key := flag.Arg(2), flag.Arg(3)
		size, err := strconv.ParseInt(flag.Arg(4), 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid size: %s\n", flag.Arg(4))
			os.Exit(1)
		}
		etag := flag.Arg(5)
		// routing key: "{bucket}/{key}"
		routingKey := bucket + "/" + key
		resp, err := client.Send(ctx, routingKey, ObjectRequest{
			Op: "put", Bucket: bucket, Key: key,
			Size: size, ETag: etag, StorageClass: "STANDARD",
		})
		if err != nil {
			slog.Error("object put failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("put: bucket=%s key=%s size=%d etag=%s last_modified=%s\n",
			resp.Bucket, resp.Key, resp.Size, resp.ETag, resp.LastModified.Format(time.RFC3339))

	case "get":
		if flag.NArg() < 4 {
			fmt.Fprintln(os.Stderr, "usage: s3_client object get <bucket> <key>")
			os.Exit(1)
		}
		bucket, key := flag.Arg(2), flag.Arg(3)
		routingKey := bucket + "/" + key
		resp, err := client.Send(ctx, routingKey, ObjectRequest{Op: "get", Bucket: bucket, Key: key})
		if err != nil {
			slog.Error("object get failed", "err", err)
			os.Exit(1)
		}
		if !resp.Found {
			fmt.Fprintf(os.Stderr, "object %s/%s not found\n", bucket, key)
			os.Exit(1)
		}
		fmt.Printf("bucket=%s key=%s size=%d etag=%s storage_class=%s last_modified=%s\n",
			resp.Bucket, resp.Key, resp.Size, resp.ETag, resp.StorageClass, resp.LastModified.Format(time.RFC3339))

	case "delete":
		if flag.NArg() < 4 {
			fmt.Fprintln(os.Stderr, "usage: s3_client object delete <bucket> <key>")
			os.Exit(1)
		}
		bucket, key := flag.Arg(2), flag.Arg(3)
		routingKey := bucket + "/" + key
		_, err := client.Send(ctx, routingKey, ObjectRequest{Op: "delete", Bucket: bucket, Key: key})
		if err != nil {
			slog.Error("object delete failed", "err", err)
			os.Exit(1)
		}
		fmt.Println("deleted")

	case "list":
		if flag.NArg() < 3 {
			fmt.Fprintln(os.Stderr, "usage: s3_client object list <bucket> [prefix]")
			os.Exit(1)
		}
		bucket := flag.Arg(2)
		objPrefix := ""
		if flag.NArg() >= 4 {
			objPrefix = flag.Arg(3)
		}
		// object routing key: "{bucket}/{key}" — bucket 내 모든 object 범위
		startKey := bucket + "/" + objPrefix
		endKey := prefixEnd(bucket + "/" + objPrefix)
		req := ObjectRequest{Op: "list", Bucket: bucket, StartKey: startKey, EndKey: endKey}
		partResults, err := client.Scan(ctx, startKey, endKey, req)
		if err != nil {
			slog.Error("object list failed", "err", err)
			os.Exit(1)
		}
		total := 0
		for _, pr := range partResults {
			for _, item := range pr.Items {
				fmt.Printf("%-50s %10d  %s  %s\n",
					item.Key, item.Size, item.ETag, item.LastModified.Format(time.RFC3339))
				total++
			}
		}
		if total == 0 {
			fmt.Println("(no objects found)")
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown object op: %s\n", op)
		os.Exit(1)
	}
}
