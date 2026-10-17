package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	w "github.com/devSparta/distributedTaskRunner/internal/worker"
	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	conn, err := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	healthClient := healthpb.NewHealthClient(conn)

	ctxConn, stop := context.WithTimeout(context.Background(), time.Second*3)
	defer stop()

	res, err := healthClient.Check(ctxConn, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		conn.Close()
		log.Fatalf("server is unavailable or does not support health check: %v", err)
	}

	if res.Status == healthpb.HealthCheckResponse_SERVING {
		log.Println("server is available")
	} else {
		conn.Close()
		log.Fatalf("server has status: %v", res.Status)
	}

	taskService := pb.NewTaskServiceClient(conn)
	runner, err := w.NewRunner(taskService, "worker_1", 3)
	if err != nil {
		conn.Close()
		log.Fatalf("could not create runner: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err = runner.Run(ctx); err != nil {
		log.Printf("could not run worker: %v", err)
	}
}
