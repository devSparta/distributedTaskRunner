package task

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskManagerAddGetDelete(t *testing.T) {
	manager := NewTaskManager()
	task := Task{ID: 1, URL: "https://example.com/path"}

	if _, ok := manager.Get(task.ID); ok {
		t.Fatal("unexpected task before add")
	}
	if err := manager.Add(task); err != nil {
		t.Fatalf("add task: %v", err)
	}
	if err := manager.Add(task); !errors.Is(err, ErrTaskAlreadyExists) {
		t.Fatalf("duplicate task: got %v, want ErrTaskAlreadyExists", err)
	}

	state, ok := manager.Get(task.ID)
	if !ok {
		t.Fatal("task not found after add")
	}
	if state.Task != task || state.Status != StatusPending || state.HasResult {
		t.Fatalf("unexpected initial state: %+v", state)
	}

	if err := manager.Delete(task.ID); err != nil {
		t.Fatalf("delete pending task: %v", err)
	}
	if _, ok := manager.Get(task.ID); ok {
		t.Fatal("task still exists after delete")
	}
	if err := manager.Delete(task.ID); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("delete missing task: got %v, want ErrTaskNotFound", err)
	}
}

func TestTaskManagerAssignAndFinish(t *testing.T) {
	manager := NewTaskManager()
	task := Task{ID: 7, URL: "https://example.com/check"}
	if err := manager.Add(task); err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	attemptID, err := manager.Assign(task.ID, "worker-1")
	if err != nil {
		t.Fatalf("assign task: %v", err)
	}
	if attemptID != 1 {
		t.Fatalf("first attempt ID: got %d, want 1", attemptID)
	}

	state, ok := manager.Get(task.ID)
	if !ok {
		t.Fatal("assigned task not found")
	}
	if state.Status != StatusRunning || state.AssignedWorkerID != "worker-1" || state.AttemptID != 1 {
		t.Fatalf("unexpected running state: %+v", state)
	}
	if state.LeaseUntil.Before(before.Add(29*time.Second)) || state.LeaseUntil.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("unexpected lease deadline: %v", state.LeaseUntil)
	}
	if _, err := manager.Assign(task.ID, "worker-2"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("assign running task: got %v, want ErrInvalidState", err)
	}
	if err := manager.Delete(task.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("delete running task: got %v, want ErrInvalidState", err)
	}

	result := Result{
		TaskID:   task.ID,
		WorkerID: "worker-1",
		Status:   200,
		Duration: 150 * time.Millisecond,
	}
	if err := manager.Finish(result, "worker-2", attemptID); !errors.Is(err, ErrWrongWorker) {
		t.Fatalf("finish by another worker: got %v, want ErrWrongWorker", err)
	}
	if err := manager.Finish(result, "worker-1", attemptID+1); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("finish with wrong attempt: got %v, want ErrStaleAttempt", err)
	}
	if err := manager.Finish(result, "worker-1", attemptID); err != nil {
		t.Fatalf("finish task: %v", err)
	}

	state, _ = manager.Get(task.ID)
	if state.Status != StatusCompleted || !state.HasResult {
		t.Fatalf("unexpected completed state: %+v", state)
	}
	if state.Result.URL != task.URL || state.Result.Status != 200 || state.Result.Duration != result.Duration {
		t.Fatalf("unexpected stored result: %+v", state.Result)
	}
	if err := manager.Finish(result, "worker-1", attemptID); err != nil {
		t.Fatalf("idempotent finish: %v", err)
	}

	changed := result
	changed.Status = 500
	if err := manager.Finish(changed, "worker-1", attemptID); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("conflicting result: got %v, want ErrResultConflict", err)
	}
	state, _ = manager.Get(task.ID)
	if state.Result.Status != 200 {
		t.Fatalf("stored result changed after conflict: %+v", state.Result)
	}
}

func TestTaskManagerFailedResultIsIdempotent(t *testing.T) {
	manager := NewTaskManager()
	if err := manager.Add(Task{ID: 8, URL: "https://example.com"}); err != nil {
		t.Fatal(err)
	}
	attemptID, err := manager.Assign(8, "worker-1")
	if err != nil {
		t.Fatal(err)
	}

	result := Result{
		TaskID:   8,
		WorkerID: "worker-1",
		Duration: 200 * time.Millisecond,
		Err:      errors.New("connection refused"),
	}
	if err := manager.Finish(result, "worker-1", attemptID); err != nil {
		t.Fatal(err)
	}

	retry := result
	retry.Err = errors.New("connection refused")
	if err := manager.Finish(retry, "worker-1", attemptID); err != nil {
		t.Fatalf("retry with equivalent error: %v", err)
	}

	retry.Err = errors.New("timeout")
	if err := manager.Finish(retry, "worker-1", attemptID); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("retry with another error: got %v, want ErrResultConflict", err)
	}

	state, _ := manager.Get(8)
	if state.Status != StatusFailed || !state.HasResult || state.Result.Err.Error() != "connection refused" {
		t.Fatalf("unexpected failed state: %+v", state)
	}
}

func TestTaskManagerMissingTask(t *testing.T) {
	manager := NewTaskManager()
	if _, err := manager.Assign(42, "worker-1"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("assign missing task: got %v, want ErrTaskNotFound", err)
	}
	if err := manager.Finish(Result{TaskID: 42}, "worker-1", 1); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("finish missing task: got %v, want ErrTaskNotFound", err)
	}
}

func TestTaskManagerRecoverExpired(t *testing.T) {
	manager := NewTaskManager()
	for _, task := range []Task{
		{ID: 1, URL: "https://example.com/expired"},
		{ID: 2, URL: "https://example.com/completed"},
		{ID: 3, URL: "https://example.com/pending"},
	} {
		if err := manager.Add(task); err != nil {
			t.Fatal(err)
		}
	}

	attemptID, err := manager.Assign(1, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	finishedAttempt, err := manager.Assign(2, "worker-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(Result{TaskID: 2, WorkerID: "worker-2", Status: 204}, "worker-2", finishedAttempt); err != nil {
		t.Fatal(err)
	}

	state, _ := manager.Get(1)
	if expired := manager.GetExpiredTasks(state.LeaseUntil.Add(-time.Nanosecond)); len(expired) != 0 {
		t.Fatalf("task expired before deadline: %+v", expired)
	}
	if expired := manager.GetExpiredTasks(state.LeaseUntil); len(expired) != 1 || expired[0].ID != 1 {
		t.Fatalf("unexpected expired tasks: %+v", expired)
	}

	calls := 0
	deadline := state.LeaseUntil.Add(time.Second)
	if recovered := manager.RecoverExpired(deadline, func(Task) bool {
		calls++
		return false
	}); recovered != 0 || calls != 1 {
		t.Fatalf("failed enqueue: recovered=%d calls=%d", recovered, calls)
	}
	state, _ = manager.Get(1)
	if state.Status != StatusRunning || state.AttemptID != attemptID {
		t.Fatalf("state changed despite full queue: %+v", state)
	}

	var enqueued []Task
	recovered := manager.RecoverExpired(deadline, func(task Task) bool {
		enqueued = append(enqueued, task)
		return true
	})
	if recovered != 1 || len(enqueued) != 1 || enqueued[0].ID != 1 {
		t.Fatalf("unexpected recovery: recovered=%d tasks=%+v", recovered, enqueued)
	}
	state, _ = manager.Get(1)
	if state.Status != StatusPending || state.AssignedWorkerID != "" || !state.LeaseUntil.IsZero() || state.AttemptID != 1 {
		t.Fatalf("unexpected recovered state: %+v", state)
	}
	if count := manager.RecoverExpired(deadline, func(Task) bool { return true }); count != 0 {
		t.Fatalf("recovered task enqueued twice: %d", count)
	}
	if expired := manager.GetExpiredTasks(deadline); len(expired) != 0 {
		t.Fatalf("expired list not cleared: %+v", expired)
	}

	nextAttempt, err := manager.Assign(1, "worker-1")
	if err != nil || nextAttempt != 2 {
		t.Fatalf("reassignment: attempt=%d error=%v", nextAttempt, err)
	}
	result := Result{TaskID: 1, WorkerID: "worker-1", Status: 200}
	if err := manager.Finish(result, "worker-1", attemptID); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("old attempt accepted: %v", err)
	}
	if err := manager.Finish(result, "worker-1", nextAttempt); err != nil {
		t.Fatalf("new attempt rejected: %v", err)
	}
}

func TestTaskManagerConcurrentAssign(t *testing.T) {
	manager := NewTaskManager()
	if err := manager.Add(Task{ID: 1, URL: "https://example.com"}); err != nil {
		t.Fatal(err)
	}

	const workers = 64
	var wg sync.WaitGroup
	var successes atomic.Int64
	errorsCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			attempt, err := manager.Assign(1, fmt.Sprintf("worker-%d", i))
			if err == nil {
				if attempt != 1 {
					errorsCh <- fmt.Errorf("successful assignment returned attempt %d", attempt)
				}
				successes.Add(1)
				return
			}
			if !errors.Is(err, ErrInvalidState) {
				errorsCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errorsCh)

	for err := range errorsCh {
		t.Error(err)
	}
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful assignments: got %d, want 1", got)
	}
}

func TestTaskManagerConcurrentFinishAndRecovery(t *testing.T) {
	manager := NewTaskManager()
	if err := manager.Add(Task{ID: 1, URL: "https://example.com"}); err != nil {
		t.Fatal(err)
	}
	attemptID, err := manager.Assign(1, "worker-1")
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	finished := make(chan error, 1)
	recovered := make(chan int, 1)

	go func() {
		<-start
		finished <- manager.Finish(Result{TaskID: 1, WorkerID: "worker-1", Status: 200}, "worker-1", attemptID)
	}()
	go func() {
		<-start
		recovered <- manager.RecoverExpired(time.Now().Add(time.Minute), func(Task) bool { return true })
	}()
	close(start)

	finishErr := <-finished
	recoveryCount := <-recovered
	state, _ := manager.Get(1)

	switch recoveryCount {
	case 0:
		if finishErr != nil || state.Status != StatusCompleted {
			t.Fatalf("finish won: error=%v state=%+v", finishErr, state)
		}
	case 1:
		if !errors.Is(finishErr, ErrWrongWorker) || state.Status != StatusPending {
			t.Fatalf("recovery won: error=%v state=%+v", finishErr, state)
		}
	default:
		t.Fatalf("unexpected recovery count: %d", recoveryCount)
	}
}
