package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
		"127.0.0.1:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to create gRPC connection: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &pb.SubmitTaskRequest{
		Url: "https://example.com",
	}

	resp, err := client.SubmitTask(ctx, req)
	if err != nil {
		log.Fatalf("failed to submit task: %v", err)
	}

	taskId := resp.GetTaskId()
	if taskId == 0 {
		log.Fatalf("failed to submit task")
	}

	taskStatus := resp.GetStatus()
	if taskStatus == "" {
		log.Fatalf("failed to submit task")
	}

	fmt.Println(taskId)
	fmt.Println(taskStatus)
}
