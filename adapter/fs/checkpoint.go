package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// CheckpointStore는 파일시스템 기반 CheckpointStore 구현체.
//
// 파일 구조:
//
//	{baseDir}/{partitionID}.snapshot
//
// 저장 시 temp 파일에 먼저 쓴 뒤 rename하여 atomic write를 보장한다.
type CheckpointStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewCheckpointStore는 baseDir에 파일시스템 CheckpointStore를 생성한다.
// baseDir이 존재하지 않으면 생성한다.
func NewCheckpointStore(baseDir string) (*CheckpointStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("fs.CheckpointStore: create base dir: %w", err)
	}
	return &CheckpointStore{baseDir: baseDir}, nil
}

// Save는 파티션의 스냅샷을 저장한다. 기존 스냅샷이 있으면 덮어쓴다.
// temp 파일에 먼저 쓴 뒤 rename하여 atomic write를 보장한다.
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

// Load는 파티션의 스냅샷을 읽어 반환한다.
// 스냅샷이 없으면 (nil, nil)을 반환한다.
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

// Delete는 파티션의 스냅샷을 삭제한다. 스냅샷이 없으면 no-op.
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
