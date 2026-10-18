package worker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/devSparta/distributedTaskRunner/internal/server"
	task "github.com/devSparta/distributedTaskRunner/internal/task"
	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newWorkerTestClient(t *testing.T) pb.TaskServiceClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterTaskServiceServer(
		grpcServer,
		server.NewTaskGRPCServer(context.Background(), task.NewTaskManager(), server.NewTaskQueue(16)),
	)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		listener.Close()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		conn.Close()
		grpcServer.Stop()
		listener.Close()
	})
	return pb.NewTaskServiceClient(conn)
}

func awaitTaskResult(t *testing.T, client pb.TaskServiceClient, taskID int64) *pb.GetTaskResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		response, err := client.GetTask(ctx, &pb.GetTaskRequest{TaskId: taskID})
		if err != nil {
			t.Fatalf("get task %d: %v", taskID, err)
		}
		if response.GetStatus() == "completed" || response.GetStatus() == "failed" {
			return response
		}

		select {
		case <-ctx.Done():
			t.Fatalf("task %d did not finish: %v", taskID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func runWorkerWithTask(t *testing.T, url string) *pb.GetTaskResponse {
	t.Helper()

	client := newWorkerTestClient(t)
	runner, err := NewRunner(client, "worker-1", 3)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runner stopped with error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("runner did not stop after cancellation")
		}
	})

	callCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	submitted, err := client.SubmitTask(callCtx, &pb.SubmitTaskRequest{Url: url})
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	return awaitTaskResult(t, client, submitted.GetTaskId())
}

func TestNewRunnerValidation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		workerID    string
		concurrency int
	}{
		{"empty worker ID", "", 1},
		{"zero concurrency", "worker-1", 0},
		{"negative concurrency", "worker-1", -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRunner(nil, tt.workerID, tt.concurrency); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}

func TestRunnerProcessesHTTPResponses(t *testing.T) {
	for _, statusCode := range []int{http.StatusOK, http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			requestSeen := make(chan string, 1)
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requestSeen <- req.Method + " " + req.URL.RequestURI()
				w.WriteHeader(statusCode)
			}))
			defer httpServer.Close()

			result := runWorkerWithTask(t, httpServer.URL+"/health?check=1")
			if result.GetStatus() != "completed" {
				t.Fatalf("HTTP %d produced task status %q", statusCode, result.GetStatus())
			}
			if got := result.GetResult(); got == nil || int(got.GetHttpStatus()) != statusCode || got.GetErrorMessage() != "" {
				t.Fatalf("unexpected HTTP result: %+v", got)
			}
			select {
			case got := <-requestSeen:
				if got != "GET /health?check=1" {
					t.Fatalf("unexpected request: %q", got)
				}
			default:
				t.Fatal("HTTP request not observed")
			}
		})
	}
}

func TestRunnerReportsHTTPFailure(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := httpServer.URL
	httpServer.Close()

	result := runWorkerWithTask(t, url)
	if result.GetStatus() != "failed" {
		t.Fatalf("task status: got %q, want failed", result.GetStatus())
	}
	if got := result.GetResult(); got == nil || got.GetHttpStatus() != 0 || got.GetErrorMessage() == "" {
		t.Fatalf("unexpected HTTP error result: %+v", got)
	}
}

func TestRunnerProcessesTasksConcurrently(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer httpServer.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	client := newWorkerTestClient(t)
	runner, err := NewRunner(client, "worker-1", 3)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runner stopped with error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("runner did not stop after cancellation")
		}
	})

	callCtx, stop := context.WithTimeout(context.Background(), 4*time.Second)
	defer stop()
	var ids []int64
	for i := 0; i < 3; i++ {
		response, err := client.SubmitTask(callCtx, &pb.SubmitTaskRequest{Url: httpServer.URL})
		if err != nil {
			t.Fatalf("submit task %d: %v", i, err)
		}
		ids = append(ids, response.GetTaskId())
	}

	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-callCtx.Done():
			t.Fatalf("only %d requests started concurrently: %v", i, callCtx.Err())
		}
	}

	close(release)
	for _, id := range ids {
		result := awaitTaskResult(t, client, id)
		if result.GetStatus() != "completed" || result.GetResult().GetHttpStatus() != http.StatusOK {
			t.Fatalf("task %d: unexpected response %+v", id, result)
		}
	}
}
