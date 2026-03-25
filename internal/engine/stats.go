package engine

import (
	"sync"
	"time"
)

// PartitionStats holds statistics for a single partition.
type PartitionStats struct {
	PartitionID string
	KeyCount    int64   // -1 if the actor does not implement Countable
	RPS         float64 // average over a 60-second sliding window
}

// rpsCounter is an RPS counter based on a 60-second sliding window.
// Manages 60 one-second buckets as a ring buffer.
type rpsCounter struct {
	mu      sync.Mutex
	buckets [60]int64
	ticks   [60]int64 // unix second corresponding to each bucket
}

func (r *rpsCounter) inc() {
	now := time.Now().Unix()
	idx := now % 60
	r.mu.Lock()
	if r.ticks[idx] != now {
		r.buckets[idx] = 0
		r.ticks[idx] = now
	}
	r.buckets[idx]++
	r.mu.Unlock()
}

// rps returns the average RPS over the most recent window seconds.
func (r *rpsCounter) rps(window int) float64 {
	if window <= 0 || window > 60 {
		window = 60
	}
	now := time.Now().Unix()
	cutoff := now - int64(window)
	r.mu.Lock()
	var total int64
	for i := 0; i < 60; i++ {
		if r.ticks[i] > cutoff {
			total += r.buckets[i]
		}
	}
	r.mu.Unlock()
	return float64(total) / float64(window)
}
