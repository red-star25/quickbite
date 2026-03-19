package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/red-star25/quickbite/inventory/internal/db"
	"github.com/red-star25/quickbite/inventory/internal/dlq"
	"github.com/red-star25/quickbite/inventory/internal/failures"
	"github.com/red-star25/quickbite/inventory/internal/kafkabus"
	"github.com/red-star25/quickbite/inventory/internal/outbox"
	"github.com/red-star25/quickbite/inventory/internal/store"
	inventoryv1 "github.com/red-star25/quickbite/proto/gen/go/inventory/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxAttempts = 8
	maxBackoff  = 5 * time.Second
)

// inventoryServer is the server implementation of the InventoryService.
type inventoryServer struct {
	// UnimplementedInventoryServiceServer is the default implementation of the InventoryServiceServer interface. So in future if we add new methods to the InventoryServiceServer interface, we can implement them here and it wont crash.
	inventoryv1.UnimplementedInventoryServiceServer

	// Mutex because we need to protect the stock map from concurrent access.
	mu sync.Mutex
	// stock map is the inventory of the products. We are using local map right now because we are not using a database. In future we can use a database to store the inventory.
	stock map[string]int32

	reservation map[int64]kafkabus.InventoryResult
	st          *store.Store
}

// ReserveStock is the method to reserve stock for a product.
// ReserveStock is now DB-backed (debug endpoint)
func (s *inventoryServer) ReserveStock(ctx context.Context, req *inventoryv1.ReserveStockRequest) (*inventoryv1.ReserveStockResponse, error) {
	if strings.TrimSpace(req.GetSku()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sku is required")
	}
	if req.GetQuantity() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be > 0")
	}

	// For debugging: treat as order_id = 0 (no reservation record). This updates stock only.
	tx, err := s.st.Begin(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "db error")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Use ReserveForOrderTx with a fake order id? Better: do a direct stock reserve (keeping it simple here)
	// We'll just simulate “orderId = -1” to record a reservation if you want; but for now, return Unimplemented.
	// (We keep gRPC running mainly as a learning tool.)
	return nil, status.Error(codes.Unimplemented, "gRPC ReserveStock is not used in Kafka flow (kept for later)")
}

func getenv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func backoffFor(attempt int) time.Duration {
	d := 200 * time.Millisecond * time.Duration(1<<(attempt-1))
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

func consumeOrders(ctx context.Context, st *store.Store, bus *kafkabus.Bus, fail *failures.Store) {
	for {
		msg, err := bus.OrdersReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("orders consumer stopped: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var evt kafkabus.OrderCreated
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			_ = dlq.Publish(ctx, bus.DLQWriter, "inventory", bus.ConsumerGroup, msg, "json unmarshal failed: "+err.Error(), 0)
			_ = bus.OrdersReader.CommitMessages(ctx, msg)
			continue
		}

		if evt.Type != "OrderCreated" || evt.EventID == "" || evt.OrderID <= 0 || strings.TrimSpace(evt.Sku) == "" || evt.Quantity <= 0 {
			_ = dlq.Publish(ctx, bus.DLQWriter, "inventory", bus.ConsumerGroup, msg, "invalid OrderCreated fields", 0)
			_ = bus.OrdersReader.CommitMessages(ctx, msg)
			continue
		}

		if err := handleOrderCreated(ctx, st, evt); err != nil {
			attempts, ferr := fail.Inc(ctx, msg.Topic, msg.Partition, msg.Offset, bus.ConsumerGroup, err.Error())
			if ferr != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if attempts >= maxAttempts {
				_ = dlq.Publish(ctx, bus.DLQWriter, "inventory", bus.ConsumerGroup, msg, "max attempts reached: "+err.Error(), attempts)
				_ = fail.Clear(ctx, msg.Topic, msg.Partition, msg.Offset, bus.ConsumerGroup)
				_ = bus.OrdersReader.CommitMessages(ctx, msg)
				continue
			}

			time.Sleep(backoffFor(attempts))
			continue // no commit => retry
		}

		_ = fail.Clear(ctx, msg.Topic, msg.Partition, msg.Offset, bus.ConsumerGroup)
		_ = bus.OrdersReader.CommitMessages(ctx, msg)
	}
}

func handleOrderCreated(ctx context.Context, st *store.Store, evt kafkabus.OrderCreated) error {
	tx, err := st.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1) Dedupe by eventId (handles Kafka redelivery)
	newEvt, err := st.MarkProcessedEventTx(ctx, tx, evt.EventID)
	if err != nil {
		return err
	}
	if !newEvt {
		// already processed exactly this event
		return tx.Commit(ctx)
	}

	// 2) If we already decided for this orderId, reuse it (handles duplicates with new eventId)
	existing, ok, err := st.GetReservationTx(ctx, tx, evt.OrderID)
	if err != nil {
		return err
	}

	var res store.Reservation
	if ok {
		res = existing
	} else {
		res, err = st.ReserveForOrderTx(ctx, tx, evt.OrderID, evt.Sku, int(evt.Quantity))
		if err != nil {
			return err
		}
	}

	// 3) Write inventory result into outbox (same tx) so publishing is guaranteed
	out := kafkabus.InventoryResult{
		// Deterministic event id => Orders can dedupe even across restarts/replays
		EventID:  fmt.Sprintf("inventory-%d", evt.OrderID),
		Time:     time.Now().UTC(),
		OrderID:  evt.OrderID,
		Reserved: res.Reserved,
	}
	if res.Reserved {
		out.Type = "InventoryReserved"
	} else {
		out.Type = "InventoryRejected"
		out.Reason = res.Reason
	}

	payload, _ := json.Marshal(out)

	if err := outbox.InsertTx(ctx, tx, outbox.Event{
		EventID: out.EventID,
		Topic:   "inventory.v1",
		Key:     strconv.FormatInt(evt.OrderID, 10),
		Payload: payload,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func main() {
	brokers := kafkabus.BrokersFromEnv(getenv("KAFKA_BROKERS", "localhost:19092"))
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/inventory?sslmode=disable")

	pool := db.MustConnectWithRetry(dsn)
	defer pool.Close()

	st := store.New(pool)
	fail := failures.New(pool)

	bus := kafkabus.New(brokers)
	defer bus.Close()

	ctx := context.Background()

	pub := &outbox.Publisher{
		DB:     pool,
		Writer: bus.InventoryWriter,
	}
	go pub.Run(ctx)

	// What we are doing here is we are registering the inventory server to the gRPC server. RegisterInventoryServiceServer is a function that registers the inventory server to the gRPC server.
	srv := &inventoryServer{
		st: st,
	}

	go consumeOrders(ctx, st, bus, fail)

	// gRPC server listens on port 9090.
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new gRPC server.
	grpcServer := grpc.NewServer()
	inventoryv1.RegisterInventoryServiceServer(grpcServer, srv)

	log.Println("inventory service listening on :9090")
	// Serve the gRPC server on the lis port.
	log.Fatal(grpcServer.Serve(lis))
}
