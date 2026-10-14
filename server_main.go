package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/devSparta/distributedTaskRunner/proto"
	"google.golang.org/grpc"
)

func main() {
	manager := NewTaskManager()

	taskServer := TaskGRPCServer{
		manager: manager,
	}

	list, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	pb.RegisterTaskServiceServer(grpcServer, &taskServer)

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
