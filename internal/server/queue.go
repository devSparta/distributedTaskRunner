package server

import (
	"context"

	t "github.com/devSparta/distributedTaskRunner/internal/task"
)

type TaskQueue struct {
	tasks chan t.Task
}

func NewTaskQueue(capacity int) *TaskQueue {
	return &TaskQueue{
		tasks: make(chan t.Task, capacity),
	}
}

func (q *TaskQueue) Add(ctx context.Context, task t.Task) error {
	select {
	case q.tasks <- task:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *TaskQueue) Get(ctx context.Context) (t.Task, error) {
	select {
	case task := <-q.tasks:
		return task, nil

	case <-ctx.Done():

		return t.Task{}, ctx.Err()
	}
}

func (q *TaskQueue) TryAdd(task t.Task) bool {
	select {
	case q.tasks <- task:
		return true

	default:
		return false
	}
}
