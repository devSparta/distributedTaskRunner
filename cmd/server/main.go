package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	pb.RegisterTaskServiceServer(grpcServer, taskServer)

	go func() {
		log.Printf("Starting grpc server at %v", list.Addr())
		if err := grpcServer.Serve(list); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	<-stop

	log.Println("Shutting down server...")
	grpcServer.GracefulStop()

}
