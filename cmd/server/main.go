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

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	taskQueue := server.NewTaskQueue(100)
	taskServer := server.NewTaskGRPCServer(ctx, manager, taskQueue)

	listener, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterTaskServiceServer(grpcServer, taskServer)

	healthServer := health.NewServer()
	healthServer.SetServingStatus(
		"",
		healthpb.HealthCheckResponse_SERVING,
	)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	recoveryDone := make(chan struct{})

	go func() {
		defer close(recoveryDone)

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				recovered := manager.RecoverExpired(
					time.Now(),
					taskQueue.TryAdd,
				)

				if recovered > 0 {
					log.Printf("Recovered expired tasks: %d", recovered)
				}
			}
		}
	}()

	serveDone := make(chan struct{})

	go func() {
		defer close(serveDone)

		log.Printf("Starting gRPC server at %s", listener.Addr())

		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("gRPC server stopped with error: %v", err)
			stop()
		}
	}()

	<-ctx.Done()

	log.Println("Shutting down server...")

	healthServer.Shutdown()

	<-recoveryDone

	gracefulDone := make(chan struct{})

	go func() {
		grpcServer.GracefulStop()
		close(gracefulDone)
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case <-gracefulDone:
		log.Println("gRPC server stopped gracefully")

	case <-timer.C:
		log.Println("Graceful shutdown timeout exceeded, forcing stop")
		grpcServer.Stop()
		<-gracefulDone
	}

	<-serveDone

	log.Println("Server stopped")
}
