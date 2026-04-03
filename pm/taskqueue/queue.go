// Package taskqueue provides a priority-based task queue for PM long-running operations.
// Tasks are executed one at a time (global serialization) in priority order:
//
//	PriorityFailover > PriorityManual > PriorityAuto
//
// Completed/failed tasks are kept in a fixed-size in-memory ring buffer.
package taskqueue

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

const historySize = 100

// Priority determines execution order. Higher value = higher priority.
type Priority int

const (
	PriorityAuto     Priority = 0 // triggered by the auto-balancer
	PriorityManual   Priority = 1 // triggered by user via abctl
	PriorityFailover Priority = 2 // triggered by node failure recovery
)

func (p Priority) String() string {
	switch p {
	case PriorityAuto:
		return "auto"
	case PriorityManual:
		return "manual"
	case PriorityFailover:
		return "failover"
	default:
		return "unknown"
	}
}

const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)

// Task represents a single unit of work.
type Task struct {
	ID          string
	Priority    Priority
	Type        string // "split" | "migrate" | "merge" | "failover"
	ActorType   string
	PartitionID string // primary partition (for display)
	Params      any    // *SplitParams | *MigrateParams | *MergeParams | *FailoverParams
	Output      any    // set by the executor to carry results back to the caller (e.g. newPartitionID for split)

	Status      string
	SubmittedAt time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
	Err         error
}

// QueueStatus is a snapshot of the queue at a point in time.
type QueueStatus struct {
	Running *Task
	Pending []Task // ordered by priority then submit time
	History []Task // most-recent first, up to historySize
}

// taskHeap is a min-heap (we negate priority for max-priority ordering).
type taskHeap []*Task

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority // higher priority first
	}
	return h[i].SubmittedAt.Before(h[j].SubmittedAt) // earlier submit first
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(x any)   { *h = append(*h, x.(*Task)) }
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return t
}

// Queue is a thread-safe priority task queue with a fixed history ring buffer.
type Queue struct {
	mu      sync.Mutex
	h       taskHeap
	running *Task
	subs    map[string]chan error // taskID → completion channel

	// ring buffer
	hist    [historySize]*Task
	histPos int // next write position
	histLen int // number of valid entries (0..historySize)

	ch chan struct{} // signals new work is available
}

// New creates a ready-to-use Queue.
func New() *Queue {
	return &Queue{
		subs: make(map[string]chan error),
		ch:   make(chan struct{}, 1),
	}
}

// Submit enqueues a new task and returns its ID. Non-blocking.
func (q *Queue) Submit(priority Priority, typ, actorType, partitionID string, params any) string {
	t := &Task{
		ID:          uuid.New().String(),
		Priority:    priority,
		Type:        typ,
		ActorType:   actorType,
		PartitionID: partitionID,
		Params:      params,
		Status:      StatusPending,
		SubmittedAt: time.Now(),
	}

	q.mu.Lock()
	heap.Push(&q.h, t)
	q.subs[t.ID] = make(chan error, 1)
	q.mu.Unlock()

	// notify worker (non-blocking)
	select {
	case q.ch <- struct{}{}:
	default:
	}

	return t.ID
}

// Wait blocks until the task identified by taskID completes or ctx is cancelled.
// Returns the task's execution error (nil on success).
// Returns ctx.Err() if context is cancelled before completion.
func (q *Queue) Wait(ctx context.Context, taskID string) error {
	q.mu.Lock()
	ch, ok := q.subs[taskID]
	q.mu.Unlock()

	if !ok {
		// Task already completed (fast path) — check history.
		if t := q.findHistory(taskID); t != nil {
			return t.Err
		}
		return nil
	}

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status returns a snapshot of the current queue state.
func (q *Queue) Status() QueueStatus {
	q.mu.Lock()
	defer q.mu.Unlock()

	var running *Task
	if q.running != nil {
		cp := *q.running
		running = &cp
	}

	pending := make([]Task, len(q.h))
	// copy and sort (heap order already satisfies priority → time)
	tmp := make(taskHeap, len(q.h))
	copy(tmp, q.h)
	heap.Init(&tmp)
	for i := range pending {
		pending[i] = *heap.Pop(&tmp).(*Task)
	}

	history := make([]Task, q.histLen)
	for i := 0; i < q.histLen; i++ {
		idx := (q.histPos - 1 - i + historySize) % historySize
		history[i] = *q.hist[idx]
	}

	return QueueStatus{
		Running: running,
		Pending: pending,
		History: history,
	}
}

// Start runs the worker goroutine. fn is called for each task in priority order.
// Blocks until ctx is cancelled.
func (q *Queue) Start(ctx context.Context, fn func(context.Context, *Task) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.ch:
			for {
				t := q.pop()
				if t == nil {
					break
				}
				q.run(ctx, t, fn)
			}
		}
	}
}

// pop takes the highest-priority task from the heap. Returns nil if empty.
func (q *Queue) pop() *Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.h) == 0 {
		return nil
	}
	return heap.Pop(&q.h).(*Task)
}

// run executes a single task and records the result.
func (q *Queue) run(ctx context.Context, t *Task, fn func(context.Context, *Task) error) {
	now := time.Now()
	t.StartedAt = &now
	t.Status = StatusRunning

	q.mu.Lock()
	q.running = t
	q.mu.Unlock()

	err := fn(ctx, t)

	finish := time.Now()
	t.FinishedAt = &finish
	t.Err = err
	if err != nil {
		t.Status = StatusFailed
	} else {
		t.Status = StatusDone
	}

	q.mu.Lock()
	q.running = nil
	q.pushHistory(t)
	ch := q.subs[t.ID]
	delete(q.subs, t.ID)
	q.mu.Unlock()

	if ch != nil {
		ch <- err
	}

	// signal worker to check for more work
	select {
	case q.ch <- struct{}{}:
	default:
	}
}

// pushHistory adds t to the ring buffer (caller must hold q.mu).
func (q *Queue) pushHistory(t *Task) {
	q.hist[q.histPos] = t
	q.histPos = (q.histPos + 1) % historySize
	if q.histLen < historySize {
		q.histLen++
	}
}

// TaskByID returns a snapshot of the task (running, pending, or history).
// Returns nil if the task is not found.
func (q *Queue) TaskByID(taskID string) *Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Check running.
	if q.running != nil && q.running.ID == taskID {
		cp := *q.running
		return &cp
	}
	// Check pending heap.
	for _, t := range q.h {
		if t.ID == taskID {
			cp := *t
			return &cp
		}
	}
	// Check history.
	for i := 0; i < q.histLen; i++ {
		idx := (q.histPos - 1 - i + historySize) % historySize
		if q.hist[idx] != nil && q.hist[idx].ID == taskID {
			cp := *q.hist[idx]
			return &cp
		}
	}
	return nil
}

// findHistory returns the task from the ring buffer, or nil.
func (q *Queue) findHistory(taskID string) *Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := 0; i < q.histLen; i++ {
		idx := (q.histPos - 1 - i + historySize) % historySize
		if q.hist[idx] != nil && q.hist[idx].ID == taskID {
			cp := *q.hist[idx]
			return &cp
		}
	}
	return nil
}
