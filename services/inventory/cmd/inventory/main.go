package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/red-star25/quickbite/inventory/internal/kafkabus"
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

func (s *inventoryServer) tryReserve(sku string, qty int) error {
	if sku == "" {
		return status.Error(codes.InvalidArgument, "sku is required")
	}
	if qty <= 0 {
		return status.Error(codes.InvalidArgument, "quantity must be greater than 0")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	available := s.stock[sku]
	if available < int32(qty) {
		return status.Error(codes.ResourceExhausted, "not enough stock")
	}

	s.stock[sku] = available - int32(qty)
	return nil
}

func getenv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func newEventID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
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
	srv := newInventoryServer()
	inventoryv1.RegisterInventoryServiceServer(grpcServer, srv)

	brokers := kafkabus.BrokersFromEnv(getenv("KAFKA_BROKERS", "localhost:19092"))
	bus := kafkabus.New(brokers)
	defer bus.Close()

	ctx := context.Background()

	go kafkabus.ConsumeOrders(ctx, bus.OrderReader, func(evt kafkabus.OrderCreated) error {
		err := srv.tryReserve(evt.Sku, evt.Quantity)
		out := kafkabus.InventoryResult{
			EventID: newEventID(),
			Time:    time.Now().UTC(),
			OrderID: evt.OrderID,
		}

		if err != nil {
			out.Type = "InventoryRejected"
			out.Reason = err.Error()
			out.Reserved = false
		} else {
			out.Type = "InventoryReserved"
			out.Reserved = true
		}

		return kafkabus.PublishInventoryResult(ctx, bus.InventoryWriter, out)
	})

	log.Println("inventory service listening on :9090")
	// Serve the gRPC server on the lis port.
	log.Fatal(grpcServer.Serve(lis))
}
