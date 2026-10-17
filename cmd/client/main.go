package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("too few arguments")
	}

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

	switch os.Args[1] {
	case "submit":
		u, errIn := url.ParseRequestURI(os.Args[2])
		if errIn != nil {
			log.Fatal(errIn)
		}

		if u.Scheme == "" || u.Host == "" {
			log.Fatal("wrong url")
		}

		if !(u.Scheme == "https" || u.Scheme == "http") {
			log.Fatal("wrong url")
		}

		req := &pb.SubmitTaskRequest{
			Url: u.String(),
		}

		resp, errIn := client.SubmitTask(ctx, req)
		if errIn != nil {
			log.Fatalf("failed to submit task: %v", errIn)
		}

		fmt.Printf("Submitted task with id: %d\n", resp.GetTaskId())

	case "get":
		id, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil || id <= 0 {
			log.Fatalf("invalid task ID: %s", os.Args[2])
		}

		resp, errIn := client.GetTask(ctx, &pb.GetTaskRequest{
			TaskId: id,
		})

		if errIn != nil {
			log.Fatalf("failed to get task: %v", errIn)
		}

		fmt.Printf("Get task with id: %d\n", resp.GetTaskId())
		fmt.Printf("Task status: %s\n", resp.Status)
		fmt.Printf("URL: %s\n", resp.Url)

		result := resp.GetResult()
		if result != nil {
			fmt.Printf("Task result:\n")
			fmt.Printf("\tHTTP Status: %d\n", result.HttpStatus)
			fmt.Printf("\tTime spent: %dms\n", result.DurationMs)

			if result.ErrorMessage != "" {
				fmt.Printf("\tTask error: %s\n", result.ErrorMessage)
			}
		} else {
			fmt.Println("Task result: not available yet")
		}

	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
