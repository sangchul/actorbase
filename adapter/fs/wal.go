// Package fs provides filesystem-based implementations of provider interfaces.
// Intended for local development, testing, and as a reference implementation.
// Not recommended for production use.
package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/sangchul/actorbase/provider"
)

// WALStore is a filesystem-based WALStore implementation.
//
// Directory structure:
//
//	{baseDir}/{partitionID}/{lsn:020d}
//
// Each WAL entry is stored as a separate file using the LSN as the filename (20-digit zero-padded).
// LSN ordering is guaranteed because os.ReadDir returns entries sorted by name.
type WALStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewWALStore creates a filesystem-based WALStore at baseDir.
// Creates baseDir if it does not exist.
func NewWALStore(baseDir string) (*WALStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("fs.WALStore: create base dir: %w", err)
	}
	return &WALStore{baseDir: baseDir}, nil
}

// AppendBatch appends multiple entries to the partition's WAL and returns each LSN.
// Assigns consecutive LSNs and writes all files under a single lock acquisition.
func (s *WALStore) AppendBatch(_ context.Context, partitionID string, data [][]byte) ([]uint64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Join(s.baseDir, partitionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("fs.WALStore: create partition dir: %w", err)
	}

	startLSN, err := s.nextLSN(dir)
	if err != nil {
		return nil, fmt.Errorf("fs.WALStore: resolve next LSN: %w", err)
	}

	lsns := make([]uint64, len(data))
	for i, d := range data {
		lsn := startLSN + uint64(i)
		path := filepath.Join(dir, lsnToName(lsn))
		if err := os.WriteFile(path, d, 0o644); err != nil {
			return nil, fmt.Errorf("fs.WALStore: write entry LSN=%d: %w", lsn, err)
		}
		lsns[i] = lsn
	}
	return lsns, nil
}

// ReadFrom returns all WAL entries with LSN >= fromLSN in order.
func (s *WALStore) ReadFrom(_ context.Context, partitionID string, fromLSN uint64) ([]provider.WALEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dir := filepath.Join(s.baseDir, partitionID)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fs.WALStore: read dir: %w", err)
	}

	var result []provider.WALEntry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lsn, err := nameToLSN(e.Name())
		if err != nil {
			continue // ignore unrelated files
		}
		if lsn < fromLSN {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("fs.WALStore: read entry %d: %w", lsn, err)
		}
		result = append(result, provider.WALEntry{LSN: lsn, Data: data})
	}
	return result, nil
}

// TrimBefore removes stale WAL entries with LSN less than lsn.
func (s *WALStore) TrimBefore(_ context.Context, partitionID string, lsn uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Join(s.baseDir, partitionID)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("fs.WALStore: read dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		entryLSN, err := nameToLSN(e.Name())
		if err != nil {
			continue
		}
		if entryLSN < lsn {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("fs.WALStore: remove entry %d: %w", entryLSN, err)
			}
		}
	}
	return nil
}

// nextLSN finds the current maximum LSN in dir and returns it incremented by 1.
// Returns 1 if no entries exist.
// The caller must hold the write lock before calling this function.
func (s *WALStore) nextLSN(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var max uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lsn, err := nameToLSN(e.Name())
		if err != nil {
			continue
		}
		if lsn > max {
			max = lsn
		}
	}
	return max + 1, nil
}

func lsnToName(lsn uint64) string {
	return fmt.Sprintf("%020d", lsn)
}

func nameToLSN(name string) (uint64, error) {
	return strconv.ParseUint(name, 10, 64)
}
