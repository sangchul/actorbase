package fs

import (
	"context"
	"bytes"
	"testing"
)

func newTestCheckpointStore(t *testing.T) *CheckpointStore {
	t.Helper()
	s, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}
	return s
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
