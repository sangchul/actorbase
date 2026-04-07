package engine

import (
	"sync/atomic"
	"time"
)

// atomicTime is an atomic wrapper for time.Time.
// Provides a typed Load() over atomic.Value to avoid repeated type assertions at call sites.
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
