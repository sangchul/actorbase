package taskqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// startQueue runs the queue worker in the background and returns a cancel func.
func startQueue(t *testing.T, q *Queue, fn func(context.Context, *Task) error) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go q.Start(ctx, fn)
	return cancel
}

// noop executor: marks task done immediately with no error.
func noopExec(_ context.Context, _ *Task) error { return nil }

// ── Submit / Wait ─────────────────────────────────────────────────────────────

func TestSubmitAndWait_success(t *testing.T) {
	q := New()
	cancel := startQueue(t, q, noopExec)
	defer cancel()

	taskID := q.Submit(PriorityManual, "split", "kv", "part-1", nil)
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	if err := q.Wait(ctx, taskID); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestSubmitAndWait_failure(t *testing.T) {
	q := New()
	boom := errors.New("boom")
	cancel := startQueue(t, q, func(_ context.Context, _ *Task) error { return boom })
	defer cancel()

	taskID := q.Submit(PriorityManual, "migrate", "kv", "part-1", nil)
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	err := q.Wait(ctx, taskID)
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestWait_contextCancellation(t *testing.T) {
	q := New()
	// Executor blocks until ctx is cancelled — so the task never finishes.
	blocker := make(chan struct{})
	cancel := startQueue(t, q, func(ctx context.Context, _ *Task) error {
		<-blocker
		return nil
	})
	defer func() {
		close(blocker)
		cancel()
	}()

	taskID := q.Submit(PriorityManual, "split", "kv", "part-1", nil)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer waitCancel()

	err := q.Wait(waitCtx, taskID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWait_alreadyCompleted(t *testing.T) {
	// Wait() on a task that finished before Wait() is called should use the
	// fast-path (findHistory) and return immediately.
	q := New()
	cancel := startQueue(t, q, noopExec)
	defer cancel()

	taskID := q.Submit(PriorityManual, "split", "kv", "part-1", nil)

	// Wait for completion first.
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	if err := q.Wait(ctx, taskID); err != nil {
		t.Fatalf("first wait: %v", err)
	}

	// Second Wait should still succeed via history fast-path.
	ctx2, done2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer done2()
	if err := q.Wait(ctx2, taskID); err != nil {
		t.Fatalf("second wait (fast-path): %v", err)
	}
}

// ── Priority ordering ─────────────────────────────────────────────────────────

func TestPriorityOrder(t *testing.T) {
	// Submit three tasks while the worker is blocked on the first one.
	// After unblocking, the remaining tasks should execute in Failover→Manual→Auto order.
	q := New()

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	var execOrder []string
	var mu sync.Mutex

	exec := func(_ context.Context, task *Task) error {
		if task.PartitionID == "blocker" {
			close(firstStarted)
			<-firstRelease
			return nil
		}
		mu.Lock()
		execOrder = append(execOrder, task.Priority.String())
		mu.Unlock()
		return nil
	}

	cancel := startQueue(t, q, exec)
	defer cancel()

	// Submit a blocker task first to occupy the worker.
	q.Submit(PriorityManual, "split", "kv", "blocker", nil)
	<-firstStarted // worker is now busy

	// Submit out-of-priority-order while worker is blocked.
	q.Submit(PriorityAuto, "split", "kv", "auto-part", nil)
	q.Submit(PriorityManual, "split", "kv", "manual-part", nil)
	q.Submit(PriorityFailover, "failover", "kv", "failover-part", nil)

	close(firstRelease) // unblock worker

	// Wait for all three to complete.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"failover", "manual", "auto"}
	if len(execOrder) != 3 {
		t.Fatalf("expected 3 tasks, got %d: %v", len(execOrder), execOrder)
	}
	for i, w := range want {
		if execOrder[i] != w {
			t.Errorf("position %d: want %q, got %q", i, w, execOrder[i])
		}
	}
}

func TestFIFOWithinSamePriority(t *testing.T) {
	q := New()

	blocker := make(chan struct{})
	var execOrder []string
	var mu sync.Mutex

	exec := func(_ context.Context, task *Task) error {
		if task.PartitionID == "blocker" {
			<-blocker
			return nil
		}
		mu.Lock()
		execOrder = append(execOrder, task.PartitionID)
		mu.Unlock()
		return nil
	}

	cancel := startQueue(t, q, exec)
	defer cancel()

	q.Submit(PriorityManual, "split", "kv", "blocker", nil)
	// Give the blocker task a moment to start.
	time.Sleep(10 * time.Millisecond)

	// Submit same-priority tasks in order.
	q.Submit(PriorityAuto, "split", "kv", "first", nil)
	time.Sleep(time.Millisecond)
	q.Submit(PriorityAuto, "split", "kv", "second", nil)
	time.Sleep(time.Millisecond)
	q.Submit(PriorityAuto, "split", "kv", "third", nil)

	close(blocker)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"first", "second", "third"}
	if len(execOrder) != 3 {
		t.Fatalf("expected 3, got %d: %v", len(execOrder), execOrder)
	}
	for i, w := range want {
		if execOrder[i] != w {
			t.Errorf("position %d: want %q, got %q", i, w, execOrder[i])
		}
	}
}

// ── Status snapshot ───────────────────────────────────────────────────────────

func TestStatus_runningAndPending(t *testing.T) {
	q := New()

	release := make(chan struct{})
	exec := func(_ context.Context, _ *Task) error {
		<-release
		return nil
	}

	cancel := startQueue(t, q, exec)
	defer func() {
		close(release)
		cancel()
	}()

	// First task will be picked up immediately and block.
	id1 := q.Submit(PriorityManual, "split", "kv", "part-1", nil)
	time.Sleep(20 * time.Millisecond)
	id2 := q.Submit(PriorityAuto, "migrate", "kv", "part-2", nil)
	time.Sleep(20 * time.Millisecond)

	s := q.Status()

	if s.Running == nil || s.Running.ID != id1 {
		t.Errorf("expected running task %s, got %v", id1, s.Running)
	}
	if s.Running.Status != StatusRunning {
		t.Errorf("expected status running, got %s", s.Running.Status)
	}
	if len(s.Pending) != 1 || s.Pending[0].ID != id2 {
		t.Errorf("expected 1 pending task %s, got %v", id2, s.Pending)
	}
}

func TestStatus_history(t *testing.T) {
	q := New()
	cancel := startQueue(t, q, noopExec)
	defer cancel()

	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, q.Submit(PriorityAuto, "split", "kv", "part", nil))
	}

	// Wait for all to complete.
	ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	for _, id := range ids {
		if err := q.Wait(ctx, id); err != nil {
			t.Fatalf("wait %s: %v", id, err)
		}
	}

	s := q.Status()
	if len(s.History) != 5 {
		t.Errorf("expected 5 history entries, got %d", len(s.History))
	}
	// Most recent first.
	if s.History[0].ID != ids[4] {
		t.Errorf("expected most recent first: want %s, got %s", ids[4], s.History[0].ID)
	}
}

// ── Ring buffer wrap-around ───────────────────────────────────────────────────

func TestHistoryRingBuffer_overflow(t *testing.T) {
	q := New()
	cancel := startQueue(t, q, noopExec)
	defer cancel()

	total := historySize + 10
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	var lastIDs []string
	for i := 0; i < total; i++ {
		id := q.Submit(PriorityAuto, "split", "kv", "part", nil)
		if i >= total-historySize {
			lastIDs = append(lastIDs, id)
		}
		if err := q.Wait(ctx, id); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}

	s := q.Status()
	if len(s.History) != historySize {
		t.Errorf("expected history capped at %d, got %d", historySize, len(s.History))
	}
	// Most recent should be the last submitted.
	if s.History[0].ID != lastIDs[len(lastIDs)-1] {
		t.Errorf("expected most recent first in history")
	}
}

// ── TaskByID ──────────────────────────────────────────────────────────────────

func TestTaskByID(t *testing.T) {
	q := New()

	release := make(chan struct{})
	exec := func(_ context.Context, _ *Task) error {
		<-release
		return nil
	}
	cancel := startQueue(t, q, exec)
	defer func() {
		close(release)
		cancel()
	}()

	id := q.Submit(PriorityManual, "migrate", "kv", "part-x", nil)
	time.Sleep(20 * time.Millisecond)

	// Should be found as running.
	task := q.TaskByID(id)
	if task == nil {
		t.Fatal("TaskByID returned nil for running task")
	}
	if task.Status != StatusRunning {
		t.Errorf("expected running, got %s", task.Status)
	}
}

func TestTaskByID_notFound(t *testing.T) {
	q := New()
	if task := q.TaskByID("nonexistent-id"); task != nil {
		t.Errorf("expected nil for unknown id, got %v", task)
	}
}

// ── Task.Output ───────────────────────────────────────────────────────────────

func TestTaskOutput(t *testing.T) {
	q := New()
	exec := func(_ context.Context, task *Task) error {
		task.Output = "new-partition-id-123"
		return nil
	}
	cancel := startQueue(t, q, exec)
	defer cancel()

	id := q.Submit(PriorityManual, "split", "kv", "part-1", nil)
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	if err := q.Wait(ctx, id); err != nil {
		t.Fatalf("wait: %v", err)
	}

	task := q.TaskByID(id)
	if task == nil {
		t.Fatal("TaskByID returned nil after completion")
	}
	out, ok := task.Output.(string)
	if !ok || out != "new-partition-id-123" {
		t.Errorf("unexpected Output: %v", task.Output)
	}
}

// ── Serialization (no concurrent execution) ───────────────────────────────────

func TestGlobalSerialization(t *testing.T) {
	q := New()
	var concurrent int32
	var mu sync.Mutex
	var violations int

	exec := func(_ context.Context, _ *Task) error {
		mu.Lock()
		concurrent++
		if concurrent > 1 {
			violations++
		}
		mu.Unlock()

		time.Sleep(5 * time.Millisecond)

		mu.Lock()
		concurrent--
		mu.Unlock()
		return nil
	}

	cancel := startQueue(t, q, exec)
	defer cancel()

	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()

	var ids []string
	for i := 0; i < 20; i++ {
		ids = append(ids, q.Submit(PriorityAuto, "split", "kv", "part", nil))
	}
	for _, id := range ids {
		if err := q.Wait(ctx, id); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}

	if violations > 0 {
		t.Errorf("detected %d concurrent executions — serialization violated", violations)
	}
}
