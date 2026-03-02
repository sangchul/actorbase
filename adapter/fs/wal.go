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

	"github.com/oomymy/actorbase/provider"
)

// WALStore는 파일시스템 기반 WALStore 구현체.
//
// 디렉토리 구조:
//
//	{baseDir}/{partitionID}/{lsn:020d}
//
// 각 WAL entry는 LSN을 파일 이름(20자리 0-padded)으로 사용하는 별도 파일로 저장된다.
// os.ReadDir이 이름순으로 정렬하여 반환하므로 LSN 순서가 보장된다.
type WALStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewWALStore는 baseDir에 파일시스템 WALStore를 생성한다.
// baseDir이 존재하지 않으면 생성한다.
func NewWALStore(baseDir string) (*WALStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("fs.WALStore: create base dir: %w", err)
	}
	return &WALStore{baseDir: baseDir}, nil
}

// Append는 파티션의 WAL에 엔트리를 추가하고 부여된 LSN을 반환한다.
func (s *WALStore) Append(_ context.Context, partitionID string, data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Join(s.baseDir, partitionID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, fmt.Errorf("fs.WALStore: create partition dir: %w", err)
	}

	lsn, err := s.nextLSN(dir)
	if err != nil {
		return 0, fmt.Errorf("fs.WALStore: resolve next LSN: %w", err)
	}

	path := filepath.Join(dir, lsnToName(lsn))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return 0, fmt.Errorf("fs.WALStore: write entry: %w", err)
	}
	return lsn, nil
}

// ReadFrom은 fromLSN 이상의 모든 WAL 엔트리를 순서대로 반환한다.
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
			continue // 관련 없는 파일 무시
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

// TrimBefore는 lsn 미만의 오래된 WAL 엔트리를 삭제한다.
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

// nextLSN은 dir에서 현재 최대 LSN을 찾아 +1을 반환한다.
// 엔트리가 없으면 1을 반환한다.
// 호출 전 반드시 write lock을 보유해야 한다.
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
