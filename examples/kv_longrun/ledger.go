package main

import (
	"encoding/json"
	"os"
	"sync"
)

// Ledger is the ground-truth record for KV state.
// It records only operations that received a successful server response, matching actual server state.
// A nil value means the key has been deleted.
type Ledger struct {
	mu   sync.RWMutex
	data map[string]*string // nil = deleted
	path string
}

// NewLedger creates an empty ledger.
func NewLedger(path string) *Ledger {
	return &Ledger{
		data: make(map[string]*string),
		path: path,
	}
}

// LoadLedger loads a ledger from a file.
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

// Set records a key-value pair in the ledger.
func (l *Ledger) Set(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := value
	l.data[key] = &v
}

// Del marks a key as deleted in the ledger.
func (l *Ledger) Del(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.data[key] = nil
}

// Snapshot returns a copy of the current ledger state.
func (l *Ledger) Snapshot() map[string]*string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	snap := make(map[string]*string, len(l.data))
	for k, v := range l.data {
		snap[k] = v
	}
	return snap
}

// Flush saves the ledger to a file.
func (l *Ledger) Flush() error {
	l.mu.RLock()
	b, err := json.Marshal(l.data)
	l.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(l.path, b, 0o644)
}
