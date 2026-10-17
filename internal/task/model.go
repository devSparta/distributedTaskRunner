package task

import (
	"time"
)

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
)

type Task struct {
	ID  int64
	URL string
}

type Result struct {
	TaskID   int64
	WorkerID string
	URL      string
	Status   int
	Duration time.Duration
	Err      error
}

type TaskState struct {
	Task             Task
	Status           TaskStatus
	Result           Result
	HasResult        bool
	AssignedWorkerID string
	LeaseUntil       time.Time
	AttemptID        int64
}
