package server

import (
	"context"
	"errors"
	"testing"

	task "github.com/devSparta/distributedTaskRunner/internal/task"
)

func TestTaskQueueFIFO(t *testing.T) {
	queue := NewTaskQueue(2)
	ctx := context.Background()
	first := task.Task{ID: 1, URL: "https://example.com/one"}
	second := task.Task{ID: 2, URL: "https://example.com/two"}

	if err := queue.Add(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add(ctx, second); err != nil {
		t.Fatal(err)
	}

	for _, want := range []task.Task{first, second} {
		got, err := queue.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("queue returned %+v, want %+v", got, want)
		}
	}
}

func TestTaskQueueTryAdd(t *testing.T) {
	queue := NewTaskQueue(1)
	first := task.Task{ID: 1}
	second := task.Task{ID: 2}

	if !queue.TryAdd(first) {
		t.Fatal("first enqueue failed")
	}
	if queue.TryAdd(second) {
		t.Fatal("enqueue succeeded while queue was full")
	}

	got, err := queue.Get(context.Background())
	if err != nil || got != first {
		t.Fatalf("get first task: got=%+v error=%v", got, err)
	}
	if !queue.TryAdd(second) {
		t.Fatal("enqueue failed after dequeue")
	}
	got, err = queue.Get(context.Background())
	if err != nil || got != second {
		t.Fatalf("get second task: got=%+v error=%v", got, err)
	}
}

func TestTaskQueueCanceledOperations(t *testing.T) {
	t.Run("get from empty queue", func(t *testing.T) {
		queue := NewTaskQueue(1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := queue.Get(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("get returned %v, want context.Canceled", err)
		}
	})

	t.Run("add to full queue", func(t *testing.T) {
		queue := NewTaskQueue(1)
		if !queue.TryAdd(task.Task{ID: 1}) {
			t.Fatal("failed to fill queue")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := queue.Add(ctx, task.Task{ID: 2})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("add returned %v, want context.Canceled", err)
		}
	})
}
