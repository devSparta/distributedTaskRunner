package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	task "github.com/devSparta/distributedTaskRunner/internal/task"
	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func newTestTaskService(t *testing.T) (pb.TaskServiceClient, *task.TaskManager, *TaskQueue, context.CancelFunc) {
	t.Helper()

	shutdownCtx, shutdown := context.WithCancel(context.Background())
	manager := task.NewTaskManager()
	queue := NewTaskQueue(16)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterTaskServiceServer(server, NewTaskGRPCServer(shutdownCtx, manager, queue))

	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		shutdown()
		server.Stop()
		listener.Close()
		t.Fatalf("create gRPC client: %v", err)
	}

	t.Cleanup(func() {
		shutdown()
		conn.Close()
		server.Stop()
		listener.Close()
	})

	return pb.NewTaskServiceClient(conn), manager, queue, shutdown
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("gRPC code: got %s, want %s; error=%v", got, want, err)
	}
}

func TestTaskServiceLifecycle(t *testing.T) {
	client, _, _, _ := newTestTaskService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	submitted, err := client.SubmitTask(ctx, &pb.SubmitTaskRequest{Url: "https://example.com/health?check=1"})
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	if submitted.GetTaskId() != 1 || submitted.GetStatus() != "pending" {
		t.Fatalf("unexpected submit response: %+v", submitted)
	}

	pending, err := client.GetTask(ctx, &pb.GetTaskRequest{TaskId: submitted.GetTaskId()})
	if err != nil {
		t.Fatal(err)
	}
	if pending.GetStatus() != "pending" || pending.GetResult() != nil {
		t.Fatalf("unexpected pending response: %+v", pending)
	}

	acquired, err := client.AcquireTask(ctx, &pb.AcquireTaskRequest{WorkerId: "worker-1"})
	if err != nil {
		t.Fatalf("acquire task: %v", err)
	}
	if acquired.GetTaskId() != submitted.GetTaskId() || acquired.GetAttemptId() != 1 || acquired.GetUrl() != pending.GetUrl() {
		t.Fatalf("unexpected acquired task: %+v", acquired)
	}

	running, err := client.GetTask(ctx, &pb.GetTaskRequest{TaskId: acquired.GetTaskId()})
	if err != nil {
		t.Fatal(err)
	}
	if running.GetStatus() != "running" || running.GetResult() != nil {
		t.Fatalf("unexpected running response: %+v", running)
	}

	report := &pb.ReportTaskResultRequest{
		TaskId:    acquired.GetTaskId(),
		WorkerId:  "worker-1",
		AttemptId: acquired.GetAttemptId(),
		Result:    &pb.TaskResult{HttpStatus: 204, DurationMs: 125},
	}
	if _, err := client.ReportTaskResult(ctx, report); err != nil {
		t.Fatalf("report result: %v", err)
	}
	if _, err := client.ReportTaskResult(ctx, report); err != nil {
		t.Fatalf("repeat report result: %v", err)
	}

	completed, err := client.GetTask(ctx, &pb.GetTaskRequest{TaskId: acquired.GetTaskId()})
	if err != nil {
		t.Fatal(err)
	}
	if completed.GetStatus() != "completed" || completed.GetUrl() != pending.GetUrl() {
		t.Fatalf("unexpected completed task: %+v", completed)
	}
	if result := completed.GetResult(); result == nil || result.GetHttpStatus() != 204 || result.GetDurationMs() != 125 || result.GetErrorMessage() != "" {
		t.Fatalf("unexpected completed result: %+v", result)
	}

	conflict := *report
	conflict.Result = &pb.TaskResult{HttpStatus: 500, DurationMs: 125}
	_, err = client.ReportTaskResult(ctx, &conflict)
	requireCode(t, err, codes.FailedPrecondition)

	otherWorker := *report
	otherWorker.WorkerId = "worker-2"
	_, err = client.ReportTaskResult(ctx, &otherWorker)
	requireCode(t, err, codes.FailedPrecondition)
}

func TestTaskServiceValidation(t *testing.T) {
	client, _, _, _ := newTestTaskService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tests := []struct {
		name string
		call func() error
		code codes.Code
	}{
		{
			name: "empty URL",
			call: func() error {
				_, err := client.SubmitTask(ctx, &pb.SubmitTaskRequest{})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "invalid task ID",
			call: func() error {
				_, err := client.GetTask(ctx, &pb.GetTaskRequest{TaskId: 0})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "missing task",
			call: func() error {
				_, err := client.GetTask(ctx, &pb.GetTaskRequest{TaskId: 100})
				return err
			},
			code: codes.NotFound,
		},
		{
			name: "empty worker ID",
			call: func() error {
				_, err := client.AcquireTask(ctx, &pb.AcquireTaskRequest{})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "missing report result",
			call: func() error {
				_, err := client.ReportTaskResult(ctx, &pb.ReportTaskResultRequest{TaskId: 1, WorkerId: "worker-1", AttemptId: 1})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "invalid attempt ID",
			call: func() error {
				_, err := client.ReportTaskResult(ctx, &pb.ReportTaskResultRequest{TaskId: 1, WorkerId: "worker-1", Result: &pb.TaskResult{}})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "report for missing task",
			call: func() error {
				_, err := client.ReportTaskResult(ctx, &pb.ReportTaskResultRequest{TaskId: 100, WorkerId: "worker-1", AttemptId: 1, Result: &pb.TaskResult{}})
				return err
			},
			code: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireCode(t, tt.call(), tt.code)
		})
	}
}

func TestTaskServiceAcquireTimeoutAndShutdown(t *testing.T) {
	client, _, _, shutdown := newTestTaskService(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := client.AcquireTask(ctx, &pb.AcquireTaskRequest{WorkerId: "worker-1"})
	requireCode(t, err, codes.DeadlineExceeded)

	shutdown()
	ctxAfterShutdown, cancelAfterShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelAfterShutdown()
	_, err = client.AcquireTask(ctxAfterShutdown, &pb.AcquireTaskRequest{WorkerId: "worker-1"})
	requireCode(t, err, codes.Unavailable)
}

func TestTaskServiceReassignsExpiredTask(t *testing.T) {
	client, manager, queue, _ := newTestTaskService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	submitted, err := client.SubmitTask(ctx, &pb.SubmitTaskRequest{Url: "https://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.AcquireTask(ctx, &pb.AcquireTaskRequest{WorkerId: "worker-1"})
	if err != nil {
		t.Fatal(err)
	}

	recovered := manager.RecoverExpired(time.Now().Add(time.Minute), queue.TryAdd)
	if recovered != 1 {
		t.Fatalf("recovered %d tasks, want 1", recovered)
	}

	second, err := client.AcquireTask(ctx, &pb.AcquireTaskRequest{WorkerId: "worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	if second.GetTaskId() != submitted.GetTaskId() || second.GetAttemptId() != first.GetAttemptId()+1 {
		t.Fatalf("unexpected reassignment: first=%+v second=%+v", first, second)
	}

	oldReport := &pb.ReportTaskResultRequest{
		TaskId:    submitted.GetTaskId(),
		WorkerId:  "worker-1",
		AttemptId: first.GetAttemptId(),
		Result:    &pb.TaskResult{HttpStatus: 200},
	}
	_, err = client.ReportTaskResult(ctx, oldReport)
	requireCode(t, err, codes.FailedPrecondition)

	newReport := *oldReport
	newReport.AttemptId = second.GetAttemptId()
	if _, err := client.ReportTaskResult(ctx, &newReport); err != nil {
		t.Fatalf("report new attempt: %v", err)
	}
}

func TestTaskErrorToGRPC(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
	}{
		{task.ErrTaskNotFound, codes.NotFound},
		{task.ErrTaskAlreadyExists, codes.AlreadyExists},
		{task.ErrInvalidState, codes.FailedPrecondition},
		{task.ErrWrongWorker, codes.FailedPrecondition},
		{task.ErrStaleAttempt, codes.FailedPrecondition},
		{task.ErrResultConflict, codes.FailedPrecondition},
		{errors.New("unexpected failure"), codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			requireCode(t, taskErrorToGRPC(fmt.Errorf("wrapped: %w", tt.err)), tt.code)
		})
	}
}
