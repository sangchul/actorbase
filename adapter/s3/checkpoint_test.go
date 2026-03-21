package s3adapter

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

const testBucket = "test-cp"

func newTestCheckpointStore(t *testing.T) *CheckpointStore {
	t.Helper()
	ctx := context.Background()

	faker := gofakes3.New(s3mem.New())
	ts := httptest.NewServer(faker.Server())
	t.Cleanup(ts.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithBaseEndpoint(ts.URL),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	return NewCheckpointStore(client, testBucket, "snap")
}

func TestCheckpointStore_SaveAndLoad(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	data := []byte("snapshot-data")
	if err := s.Save(ctx, "p1", data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Load = %q, want %q", got, data)
	}
}

func TestCheckpointStore_Load_NotExist(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	data, err := s.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Load on nonexistent partition should not error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil for nonexistent partition, got %q", data)
	}
}

func TestCheckpointStore_Save_Overwrites(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	s.Save(ctx, "p1", []byte("v1"))
	s.Save(ctx, "p1", []byte("v2"))

	got, err := s.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("expected v2 after overwrite, got %q", got)
	}
}

func TestCheckpointStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	s.Save(ctx, "p1", []byte("data"))

	if err := s.Delete(ctx, "p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err := s.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("Load after delete: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after delete, got %q", got)
	}
}

func TestCheckpointStore_Delete_NonExistent(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	if err := s.Delete(ctx, "nonexistent"); err != nil {
		t.Errorf("Delete on nonexistent partition should not error: %v", err)
	}
}

func TestCheckpointStore_PartitionsAreIsolated(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	s.Save(ctx, "p1", []byte("p1-snap"))
	s.Save(ctx, "p2", []byte("p2-snap"))

	p1, _ := s.Load(ctx, "p1")
	p2, _ := s.Load(ctx, "p2")

	if string(p1) != "p1-snap" {
		t.Errorf("p1 isolation failed: got %q", p1)
	}
	if string(p2) != "p2-snap" {
		t.Errorf("p2 isolation failed: got %q", p2)
	}
}

func TestCheckpointStore_BinaryData(t *testing.T) {
	ctx := context.Background()
	s := newTestCheckpointStore(t)

	// [8바이트 LSN] + [스냅샷] 바이너리 포맷 무결성 테스트
	lsn := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A} // LSN=42
	snapshot := []byte{0xFF, 0xFE, 0x00, 0x01, 0x7B, 0x22, 0x6B, 0x22} // 임의 바이너리
	data := append(lsn, snapshot...)

	if err := s.Save(ctx, "binary-partition", data); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(ctx, "binary-partition")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("binary data mismatch: got %x, want %x", got, data)
	}
}
