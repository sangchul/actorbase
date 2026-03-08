package main

import (
	"encoding/json"
	"os"
	"sync"
)

// Ledger는 KV 정답지다.
// 서버 응답이 성공한 연산만 기록하여 실제 서버 상태와 일치한다.
// nil 값은 해당 키가 삭제되었음을 의미한다.
type Ledger struct {
	mu   sync.RWMutex
	data map[string]*string // nil = deleted
	path string
}

// NewLedger는 빈 정답지를 생성한다.
func NewLedger(path string) *Ledger {
	return &Ledger{
		data: make(map[string]*string),
		path: path,
	}
}

// LoadLedger는 파일에서 정답지를 불러온다.
func LoadLedger(path string) (*Ledger, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	l := &Ledger{
		path: path,
		data: make(map[string]*string),
	}
	if err := json.Unmarshal(b, &l.data); err != nil {
		return nil, err
	}
	return l, nil
}

// Set은 키-값을 정답지에 기록한다.
func (l *Ledger) Set(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := value
	l.data[key] = &v
}

// Del은 키를 삭제 상태로 표시한다.
func (l *Ledger) Del(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.data[key] = nil
}

// Snapshot은 현재 정답지의 복사본을 반환한다.
func (l *Ledger) Snapshot() map[string]*string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	snap := make(map[string]*string, len(l.data))
	for k, v := range l.data {
		snap[k] = v
	}
	return snap
}

// Flush는 정답지를 파일에 저장한다.
func (l *Ledger) Flush() error {
	l.mu.RLock()
	b, err := json.Marshal(l.data)
	l.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(l.path, b, 0o644)
}
