package fs

import (
	"context"
	"testing"
)

func newTestWALStore(t *testing.T) *WALStore {
	t.Helper()
	s, err := NewWALStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewWALStore: %v", err)
	}
	return s
}

func TestWALStore_AppendMonotonicallyIncreasesLSN(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsn1, _ := s.Append(ctx, "p1", []byte("a"))
	lsn2, _ := s.Append(ctx, "p1", []byte("b"))
	lsn3, _ := s.Append(ctx, "p1", []byte("c"))

	if !(lsn1 < lsn2 && lsn2 < lsn3) {
		t.Errorf("LSN not monotonically increasing: %d, %d, %d", lsn1, lsn2, lsn3)
	}
}

func TestWALStore_ReadFrom_All(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsn1, _ := s.Append(ctx, "p1", []byte("entry-1"))
	s.Append(ctx, "p1", []byte("entry-2"))
	s.Append(ctx, "p1", []byte("entry-3"))

	entries, err := s.ReadFrom(ctx, "p1", lsn1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if string(entries[0].Data) != "entry-1" || string(entries[2].Data) != "entry-3" {
		t.Errorf("unexpected data: %q, %q", entries[0].Data, entries[2].Data)
	}
}

func TestWALStore_ReadFrom_Partial(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	s.Append(ctx, "p1", []byte("a"))
	lsn2, _ := s.Append(ctx, "p1", []byte("b"))
	s.Append(ctx, "p1", []byte("c"))

	entries, err := s.ReadFrom(ctx, "p1", lsn2)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries from lsn2, got %d", len(entries))
	}
	if string(entries[0].Data) != "b" {
		t.Errorf("expected b, got %q", entries[0].Data)
	}
}

func TestWALStore_ReadFrom_EmptyPartition(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	entries, err := s.ReadFrom(ctx, "nonexistent", 1)
	if err != nil {
		t.Fatalf("ReadFrom on nonexistent partition should not error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestWALStore_TrimBefore(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	s.Append(ctx, "p1", []byte("a"))
	s.Append(ctx, "p1", []byte("b"))
	lsn3, _ := s.Append(ctx, "p1", []byte("c"))
	s.Append(ctx, "p1", []byte("d"))

	if err := s.TrimBefore(ctx, "p1", lsn3); err != nil {
		t.Fatalf("TrimBefore: %v", err)
	}

	entries, err := s.ReadFrom(ctx, "p1", 1)
	if err != nil {
		t.Fatalf("ReadFrom after trim: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after trim, got %d", len(entries))
	}
	if string(entries[0].Data) != "c" {
		t.Errorf("first remaining entry should be c, got %q", entries[0].Data)
	}
}

func TestWALStore_TrimBefore_NonexistentPartition(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	if err := s.TrimBefore(ctx, "nonexistent", 10); err != nil {
		t.Errorf("TrimBefore on nonexistent partition should not error: %v", err)
	}
}

func TestWALStore_PartitionsAreIsolated(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	s.Append(ctx, "p1", []byte("p1-data"))
	s.Append(ctx, "p2", []byte("p2-data"))

	p1, _ := s.ReadFrom(ctx, "p1", 1)
	p2, _ := s.ReadFrom(ctx, "p2", 1)

	if len(p1) != 1 || string(p1[0].Data) != "p1-data" {
		t.Errorf("p1 isolation failed: %v", p1)
	}
	if len(p2) != 1 || string(p2[0].Data) != "p2-data" {
		t.Errorf("p2 isolation failed: %v", p2)
	}
}

func TestWALStore_AppendAfterTrim(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	s.Append(ctx, "p1", []byte("a"))
	lsn2, _ := s.Append(ctx, "p1", []byte("b"))

	s.TrimBefore(ctx, "p1", lsn2)

	lsn3, err := s.Append(ctx, "p1", []byte("c"))
	if err != nil {
		t.Fatalf("Append after trim: %v", err)
	}
	if lsn3 <= lsn2 {
		t.Errorf("LSN after trim should be greater than previous: lsn2=%d lsn3=%d", lsn2, lsn3)
	}
}
