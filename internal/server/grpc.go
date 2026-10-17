package server

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	t "github.com/devSparta/distributedTaskRunner/internal/task"
	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TaskGRPCServer struct {
	pb.UnimplementedTaskServiceServer
	manager *t.TaskManager
	queue   *TaskQueue
	nextId  atomic.Int64
}

func NewTaskGRPCServer(manager *t.TaskManager, queue *TaskQueue) *TaskGRPCServer {
	return &TaskGRPCServer{
		manager: manager,
		queue:   queue,
	}
}

func (s *TaskGRPCServer) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	url := req.GetUrl()
	if url == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty url")
	}

	id := s.nextId.Add(1)

	task := &t.Task{
		ID:  id,
		URL: url,
	}

	err := s.manager.Add(*task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add task to task manager: %v", err)
	}

	err = s.queue.Add(ctx, *task)
	if err != nil {
		inErr := s.manager.Delete(id)
		if inErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete task from task manager: %v", inErr)
		}

		if errors.Is(err, context.Canceled) {
			return nil, status.Errorf(codes.Canceled, "task adding to queue is canceled")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "task adding to queue is exceeded")
		}

		return nil, status.Errorf(codes.Internal, "failed to add task to queue: %v", err)
	}

	return &pb.SubmitTaskResponse{
		TaskId: id,
		Status: "pending",
	}, nil
}

func (s *TaskGRPCServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	id := req.GetTaskId()
	if id <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty id")
	}

	taskState, ok := s.manager.Get(id)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task not found")
	}

	taskStatus := string(taskState.Status)

	taskResult := taskState.Result

	resp := &pb.GetTaskResponse{
		TaskId: id,
		Url:    taskState.Task.URL,
		Status: taskStatus,
	}
	if taskState.HasResult {
		res := &pb.TaskResult{
			HttpStatus: int32(taskResult.Status),
			DurationMs: taskResult.Duration.Milliseconds(),
		}

		if taskResult.Err != nil {
			res.ErrorMessage = taskResult.Err.Error()
		}

		resp.Result = res
	}

	return resp, nil
}

func (s *TaskGRPCServer) AcquireTask(ctx context.Context, req *pb.AcquireTaskRequest) (*pb.AcquireTaskResponse, error) {
	workerId := req.GetWorkerId()
	if workerId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty worker id")
	}

	task, err := s.queue.Get(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if err = s.manager.Assign(task.ID, workerId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to assign task %d: %v", task.ID, err)
	}

	resp := &pb.AcquireTaskResponse{
		TaskId: task.ID,
		Url:    task.URL,
	}

	return resp, nil
}

func (s *TaskGRPCServer) ReportTaskResult(ctx context.Context, req *pb.ReportTaskResultRequest) (*pb.ReportTaskResultResponse, error) {
	taskID := req.GetTaskId()
	if taskID <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty task id")
	}

	workerId := req.GetWorkerId()
	if workerId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty worker id")
	}

	resultFromReq := req.GetResult()
	if resultFromReq == nil {
		return nil, status.Errorf(codes.InvalidArgument, "empty result")
	}

	result := &t.Result{
		TaskID:   taskID,
		WorkerID: workerId,
		Status:   int(resultFromReq.HttpStatus),
		Duration: time.Millisecond * time.Duration(resultFromReq.DurationMs),
	}

	errToResult := resultFromReq.ErrorMessage
	if errToResult != "" {
		result.Err = errors.New(errToResult)
	}

	if err := s.manager.Finish(*result, workerId); err != nil {
		//добавить различие ошибок
		return nil, status.Errorf(codes.Internal, "failed to finish task %d: %v", taskID, err)
	}

	return &pb.ReportTaskResultResponse{}, nil
}
