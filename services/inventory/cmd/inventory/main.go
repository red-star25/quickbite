package main

import (
	"context"
	"log"
	"net"
	"sync"

	inventoryv1 "github.com/red-star25/quickbite/proto/gen/go/inventory/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// inventoryServer is the server implementation of the InventoryService.
type inventoryServer struct {
	// UnimplementedInventoryServiceServer is the default implementation of the InventoryServiceServer interface. So in future if we add new methods to the InventoryServiceServer interface, we can implement them here and it wont crash.
	inventoryv1.UnimplementedInventoryServiceServer

	// Mutex because we need to protect the stock map from concurrent access.
	mu sync.Mutex
	// stock map is the inventory of the products. We are using local map right now because we are not using a database. In future we can use a database to store the inventory.
	stock map[string]int32
}

// Creating constructor function to create a new inventory server.
func newInventoryServer() *inventoryServer {
	return &inventoryServer{
		stock: map[string]int32{
			"burger": 5,
			"pizza":  10,
		},
	}
}

// ReserveStock is the method to reserve stock for a product.
func (s *inventoryServer) ReserveStock(ctx context.Context, req *inventoryv1.ReserveStockRequest) (*inventoryv1.ReserveStockResponse, error) {
	// If the sku is empty, return an error.
	if req.GetSku() == "" {
		return nil, status.Error(codes.InvalidArgument, "sku is required")
	}
	// If the quantity is less than or equal to 0, return an error.
	if req.GetQuantity() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be greater than 0")
	}

	// Lock the mutex to protect the stock map from concurrent access.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the available stock for the product.
	available := s.stock[req.Sku]
	// If the available stock is less than the quantity requested, return an error.
	if available < req.Quantity {
		// Return a resource exhausted error because the stock is not enough.
		return nil, status.Error(codes.ResourceExhausted, "not enough stock")
	}

	// Update the stock for the product.
	s.stock[req.Sku] = available - req.Quantity
	// Return a reserved stock response.
	return &inventoryv1.ReserveStockResponse{Reserved: true}, nil
}

func main() {
	// gRPC server listens on port 9090.
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new gRPC server.
	grpcServer := grpc.NewServer()
	// What we are doing here is we are registering the inventory server to the gRPC server. RegisterInventoryServiceServer is a function that registers the inventory server to the gRPC server.
	inventoryv1.RegisterInventoryServiceServer(grpcServer, newInventoryServer())

	log.Println("inventory service listening on :9090")
	// Serve the gRPC server on the lis port.
	log.Fatal(grpcServer.Serve(lis))
}
