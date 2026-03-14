package engine

import (
	"sync"
	"time"
)

// PartitionStats는 파티션 하나의 통계.
type PartitionStats struct {
	PartitionID string
	KeyCount    int64   // -1이면 actor가 Countable을 구현하지 않음
	RPS         float64 // 최근 60초 슬라이딩 윈도우 평균
}

// rpsCounter는 60초 슬라이딩 윈도우 기반 RPS 카운터.
// 1초 단위 버킷 60개를 링 버퍼로 관리한다.
type rpsCounter struct {
	mu      sync.Mutex
	buckets [60]int64
	ticks   [60]int64 // 버킷에 해당하는 unix second
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

// rps는 최근 window초 평균 RPS를 반환한다.
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
