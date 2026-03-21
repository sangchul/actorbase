package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func newTestWALStore(t *testing.T) *WALStore {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	return NewWALStore(client, "wal")
}

func appendOne(t *testing.T, s *WALStore, partitionID string, data []byte) uint64 {
	t.Helper()
	lsns, err := s.AppendBatch(context.Background(), partitionID, [][]byte{data})
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	return lsns[0]
}

func TestWALStore_AppendBatch_MonotonicallyIncreasesLSN(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsns, err := s.AppendBatch(ctx, "p1", [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if len(lsns) != 3 {
		t.Fatalf("expected 3 LSNs, got %d", len(lsns))
	}
	if !(lsns[0] < lsns[1] && lsns[1] < lsns[2]) {
		t.Errorf("LSN not monotonically increasing: %v", lsns)
	}
}

func TestWALStore_AppendBatch_Empty(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsns, err := s.AppendBatch(ctx, "p1", nil)
	if err != nil {
		t.Fatalf("AppendBatch with nil: %v", err)
	}
	if len(lsns) != 0 {
		t.Errorf("expected 0 LSNs, got %d", len(lsns))
	}
}

func TestWALStore_ReadFrom_All(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsns, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("entry-1"), []byte("entry-2"), []byte("entry-3")})

	entries, err := s.ReadFrom(ctx, "p1", lsns[0])
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

	lsns, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("a"), []byte("b"), []byte("c")})

	entries, err := s.ReadFrom(ctx, "p1", lsns[1])
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries from lsns[1], got %d", len(entries))
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

	lsns, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})

	if err := s.TrimBefore(ctx, "p1", lsns[2]); err != nil {
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

	s.AppendBatch(ctx, "p1", [][]byte{[]byte("p1-data")})
	s.AppendBatch(ctx, "p2", [][]byte{[]byte("p2-data")})

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

	lsns1, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("a"), []byte("b")})
	s.TrimBefore(ctx, "p1", lsns1[1])

	lsns2, err := s.AppendBatch(ctx, "p1", [][]byte{[]byte("c")})
	if err != nil {
		t.Fatalf("AppendBatch after trim: %v", err)
	}
	if lsns2[0] <= lsns1[1] {
		t.Errorf("LSN after trim should be greater than previous: lsns1[1]=%d lsns2[0]=%d", lsns1[1], lsns2[0])
	}
}

func TestWALStore_AppendBatch_LSNContinuousAcrossBatches(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsns1, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("a"), []byte("b")})
	lsns2, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("c"), []byte("d")})

	if lsns2[0] != lsns1[1]+1 {
		t.Errorf("LSN should be continuous across batches: lsns1=%v lsns2=%v", lsns1, lsns2)
	}
}

func TestWALStore_ReadFrom_LSNsMatch(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	lsns, _ := s.AppendBatch(ctx, "p1", [][]byte{[]byte("x"), []byte("y"), []byte("z")})

	entries, err := s.ReadFrom(ctx, "p1", lsns[0])
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	for i, e := range entries {
		if e.LSN != lsns[i] {
			t.Errorf("entry[%d].LSN = %d, want %d", i, e.LSN, lsns[i])
		}
	}
}

func TestWALStore_BinaryData(t *testing.T) {
	ctx := context.Background()
	s := newTestWALStore(t)

	original := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80}
	lsns, _ := s.AppendBatch(ctx, "p1", [][]byte{original})

	entries, err := s.ReadFrom(ctx, "p1", lsns[0])
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if string(entries[0].Data) != string(original) {
		t.Errorf("binary data mismatch: got %v, want %v", entries[0].Data, original)
	}
}
