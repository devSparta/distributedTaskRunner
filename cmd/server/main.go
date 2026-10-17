package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/devSparta/distributedTaskRunner/internal/server"
	t "github.com/devSparta/distributedTaskRunner/internal/task"
	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	manager := t.NewTaskManager()

	taskQueue := server.NewTaskQueue(100)
	taskServer := server.NewTaskGRPCServer(manager, taskQueue)

	list, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	healthServer := health.NewServer()

	healthServer.SetServingStatus(
		"",
		healthpb.HealthCheckResponse_SERVING,
	)

	healthpb.RegisterHealthServer(grpcServer, healthServer)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pb.RegisterTaskServiceServer(grpcServer, taskServer)

	go func() {
		log.Printf("Starting grpc server at %v", list.Addr())
		if err := grpcServer.Serve(list); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				recovered := manager.RecoverExpired(time.Now(), taskQueue.TryAdd)
				if recovered > 0 {
					log.Printf("Recovered expired tasks: %d", recovered)
				}
			}
		}
	}()

	<-ctx.Done()

	log.Println("Shutting down server...")
	grpcServer.GracefulStop()
}
