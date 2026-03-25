package engine

import (
	"sync/atomic"
	"time"
)

// atomicTime is an atomic wrapper for time.Time.
type atomicTime struct {
	v atomic.Value
}

func (a *atomicTime) Store(t time.Time) {
	a.v.Store(t)
}

func (a *atomicTime) Load() time.Time {
	v := a.v.Load()
	if v == nil {
		return time.Time{}
	}
	return v.(time.Time)
}

// atomicUint64 is an atomic wrapper for uint64.
type atomicUint64 struct {
	v atomic.Uint64
}

func (a *atomicUint64) Store(val uint64) {
	a.v.Store(val)
}

func (a *atomicUint64) Load() uint64 {
	return a.v.Load()
}
