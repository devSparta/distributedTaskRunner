package task

import (
	"fmt"
	"sync"
	"time"
)

type TaskManager struct {
	mu    sync.RWMutex
	tasks map[int64]TaskState
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[int64]TaskState),
	}
}

func (m *TaskManager) Add(task Task) error {
	state := TaskState{
		Task:   task,
		Status: StatusPending,
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tasks[task.ID]; ok {
		return fmt.Errorf(
			"task %d: %w",
			task.ID,
			ErrTaskAlreadyExists,
		)
	}

	m.tasks[task.ID] = state

	return nil
}

func (m *TaskManager) Finish(
	result Result,
	workerID string,
	attemptID int64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := result.TaskID

	taskState, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task %d: %w", id, ErrTaskNotFound)
	}

	if taskState.AssignedWorkerID != workerID {
		return fmt.Errorf(
			"cannot finish task %d: assigned worker %q, got %q: %w",
			id,
			taskState.AssignedWorkerID,
			workerID,
			ErrWrongWorker,
		)
	}

	if taskState.AttemptID != attemptID {
		return fmt.Errorf(
			"task %d: expected attempt %d, got %d: %w",
			id,
			taskState.AttemptID,
			attemptID,
			ErrStaleAttempt,
		)
	}

	if taskState.Status == StatusCompleted ||
		taskState.Status == StatusFailed {

		if !taskState.HasResult {
			return fmt.Errorf(
				"task %d is finished but has no result: %w",
				id,
				ErrInvalidState,
			)
		}

		old := taskState.Result

		sameError := (old.Err == nil && result.Err == nil) ||
			(old.Err != nil &&
				result.Err != nil &&
				old.Err.Error() == result.Err.Error())

		sameResult := old.TaskID == result.TaskID &&
			old.WorkerID == result.WorkerID &&
			old.Status == result.Status &&
			old.Duration == result.Duration &&
			sameError

		if !sameResult {
			return fmt.Errorf(
				"task %d already has a different result: %w",
				id,
				ErrResultConflict,
			)
		}

		return nil
	}

	if taskState.Status != StatusRunning {
		return fmt.Errorf(
			"cannot finish task %d with status %s: %w",
			id,
			taskState.Status,
			ErrInvalidState,
		)
	}

	if result.Err == nil {
		taskState.Status = StatusCompleted
	} else {
		taskState.Status = StatusFailed
	}

	result.URL = taskState.Task.URL
	taskState.Result = result
	taskState.HasResult = true

	m.tasks[id] = taskState

	return nil
}

func (m *TaskManager) Get(id int64) (TaskState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.tasks[id]
	if !ok {
		return TaskState{}, false
	}

	return state, true
}

func (m *TaskManager) Delete(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task %d: %w", id, ErrTaskNotFound)
	}

	if state.Status != StatusPending {
		return fmt.Errorf(
			"cannot delete task %d with status %s: %w",
			id,
			state.Status,
			ErrInvalidState,
		)
	}

	delete(m.tasks, id)

	return nil
}

func (m *TaskManager) Assign(
	id int64,
	workerID string,
) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.tasks[id]
	if !ok {
		return 0, fmt.Errorf("task %d: %w", id, ErrTaskNotFound)
	}

	if state.Status != StatusPending {
		return 0, fmt.Errorf(
			"cannot assign task %d with status %s: %w",
			id,
			state.Status,
			ErrInvalidState,
		)
	}

	state.AssignedWorkerID = workerID
	state.Status = StatusRunning
	state.AttemptID++
	state.LeaseUntil = time.Now().Add(30 * time.Second)

	m.tasks[id] = state

	return state.AttemptID, nil
}

func (m *TaskManager) GetExpiredTasks(now time.Time) []Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Task, 0, len(m.tasks))

	for _, state := range m.tasks {
		if state.Status == StatusRunning &&
			!state.LeaseUntil.After(now) {
			result = append(result, state.Task)
		}
	}

	return result
}

func (m *TaskManager) RecoverExpired(
	now time.Time,
	tryEnqueue func(Task) bool,
) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	correctlyRecovered := 0

	for id, state := range m.tasks {
		if state.Status != StatusRunning ||
			state.LeaseUntil.After(now) {
			continue
		}

		if tryEnqueue(state.Task) {
			state.Status = StatusPending
			state.AssignedWorkerID = ""
			state.LeaseUntil = time.Time{}

			m.tasks[id] = state
			correctlyRecovered++
		}
	}

	return correctlyRecovered
}
