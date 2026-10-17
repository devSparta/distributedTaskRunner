package task

import "time"

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
