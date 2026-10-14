package main

import (
	"context"
	"sync/atomic"

	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TaskGRPCServer struct {
	pb.UnimplementedTaskServiceServer
	manager *TaskManager
	nextId  atomic.Int64
}

func (s *TaskGRPCServer) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	url := req.GetUrl()
	if url == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty url")
	}

	id := s.nextId.Add(1)

	task := &Task{
		ID:  id,
		URL: url,
	}

	err := s.manager.Add(*task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add task: %v", err)
	}

	return &pb.SubmitTaskResponse{
		TaskId: id,
		Status: "pending",
	}, nil
}
