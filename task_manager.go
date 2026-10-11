package main

import (
	"fmt"
	"sync"
)

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
)

type TaskState struct {
	Task      Task
	Status    TaskStatus
	Result    Result
	HasResult bool
}

type TaskManager struct {
	mu    sync.RWMutex
	tasks map[int]TaskState
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[int]TaskState),
	}
}

func (m *TaskManager) Add(task Task) error {
	state := TaskState{
		Task:   task,
		Status: StatusPending,
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.tasks[task.ID]

	if ok {
		return fmt.Errorf("task already exists: %v", task)
	}

	m.tasks[task.ID] = state

	return nil
}

func (m *TaskManager) Start(id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task does not exist: %v", id)
	}

	if task.Status != StatusPending {
		return fmt.Errorf("task already started: %v", task)
	}

	task.Status = StatusRunning

	m.tasks[id] = task

	return nil
}

func (m *TaskManager) Finish(result Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := result.TaskID

	task, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task does not exist: %v", id)
	}

	if task.Status != StatusRunning {
		return fmt.Errorf("cannot finish task %d with status %s", id, task.Status)
	}

	if result.Err == nil {
		task.Status = StatusCompleted
	} else {
		task.Status = StatusFailed
	}

	task.Result = result
	task.HasResult = true

	m.tasks[id] = task

	return nil
}

func (m *TaskManager) Get(id int) (TaskState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.tasks[id]
	if !ok {
		return TaskState{}, false
	}

	return state, true
}
