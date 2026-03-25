package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// CheckpointStore is a filesystem-based CheckpointStore implementation.
//
// File structure:
//
//	{baseDir}/{partitionID}.snapshot
//
// Writes first to a temp file then renames it to guarantee atomic writes.
type CheckpointStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewCheckpointStore creates a filesystem-based CheckpointStore at baseDir.
// Creates baseDir if it does not exist.
func NewCheckpointStore(baseDir string) (*CheckpointStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("fs.CheckpointStore: create base dir: %w", err)
	}
	return &CheckpointStore{baseDir: baseDir}, nil
}

// Save stores a snapshot of the partition. Overwrites any existing snapshot.
// Writes first to a temp file then renames it to guarantee atomic writes.
func (s *CheckpointStore) Save(_ context.Context, partitionID string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.snapshotPath(partitionID)
	tmp := path + ".tmp"

	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("fs.CheckpointStore: write temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("fs.CheckpointStore: rename: %w", err)
	}
	return nil
}

// Load reads and returns a snapshot of the partition.
// Returns (nil, nil) if no snapshot exists.
func (s *CheckpointStore) Load(_ context.Context, partitionID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.snapshotPath(partitionID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fs.CheckpointStore: read: %w", err)
	}
	return data, nil
}

// Delete removes the snapshot for the partition. No-op if no snapshot exists.
func (s *CheckpointStore) Delete(_ context.Context, partitionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(s.snapshotPath(partitionID))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("fs.CheckpointStore: remove: %w", err)
	}
	return nil
}

func (s *CheckpointStore) snapshotPath(partitionID string) string {
	return filepath.Join(s.baseDir, partitionID+".snapshot")
}
