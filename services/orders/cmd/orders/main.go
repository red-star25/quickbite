package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/red-star25/quickbite/orders/internal/db"
	"github.com/red-star25/quickbite/orders/internal/failures"
	httpapi "github.com/red-star25/quickbite/orders/internal/http"
	inv "github.com/red-star25/quickbite/orders/internal/inventory"
	"github.com/red-star25/quickbite/orders/internal/kafkabus"
	"github.com/red-star25/quickbite/orders/internal/outbox"
	"github.com/red-star25/quickbite/orders/internal/store"
)

func main() {
	// Read config from env
	// try to read port from env, if not set, use 8080
	port := getenv("PORT", "8080")
	// dsn - Data Source Name. Its a database connection string.
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable")
	// inventory service address.
	invAddr := getenv("INVENTORY_GRPC_ADDR", "localhost:9090")
	/*
		Connects to Postgres.
		"With retry" means it keepstrying for a bit if the DB isn't ready yet.
		"must" means if it fails, the program exists/crashes
	*/
	pool := db.MustConnectWithRetry(dsn)
	defer pool.Close()

	// Create a new inventory client.
	invClient, err := inv.New(invAddr)
	if err != nil {
		log.Fatalf("failed to create inventory client: %v", err)
	}
	defer invClient.Close()

	orderStore := store.NewOrdersStore(pool)

	brokers := kafkabus.BrokersFromEnv(getenv("KAFKA_BROKERS", "localhost:19092"))
	bus := kafkabus.New(brokers)
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := &outbox.Publisher{
		DB:     pool,
		Writer: bus.OrderWriter,
	}
	go pub.Run(ctx)

	fail := failures.New(pool)

	go kafkabus.ConsumeInventoryResults(ctx, bus.InventoryReader, bus.ConsumerGroup, bus.DLQWriter, fail, func(evt kafkabus.InventoryResult) error {
		tx, err := orderStore.Begin(ctx)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(ctx)
			}
		}()

		log.Printf("orders: inventory handler starting eventId=%s type=%s orderId=%d reserved=%v", evt.EventID, evt.Type, evt.OrderID, evt.Reserved)
		newEvt, err := orderStore.MarkProcessedEventTx(ctx, tx, evt.EventID)
		if err != nil {
			return err
		}
		log.Printf("orders: inventory handler processed_event_insert eventId=%s inserted=%v", evt.EventID, newEvt)
		if !newEvt {
			// Event was already processed in a prior successful transaction.
			// Commit to close out this (read-only) transaction.
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			committed = true
			log.Printf("orders: inventory handler skipping DB updates for already-processed eventId=%s", evt.EventID)
			return nil
		}

		// At this point we know the event row was inserted into processed_events.
		switch evt.Type {
		case "InventoryReserved":
			if err := orderStore.UpdateStatusTx(ctx, tx, evt.OrderID, store.StatusConfirmed); err != nil {
				return err
			}
			log.Printf("orders: inventory handler updated order status orderId=%d status=%s", evt.OrderID, store.StatusConfirmed)
		case "InventoryRejected":
			if err := orderStore.UpdateStatusTx(ctx, tx, evt.OrderID, store.StatusCancelled); err != nil {
				return err
			}
			log.Printf("orders: inventory handler updated order status orderId=%d status=%s", evt.OrderID, store.StatusCancelled)
		default:
			// Defensive: shouldn't happen because ConsumeInventoryResults already validates evt.Type.
			return errors.New("invalid inventory event type: " + evt.Type)
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		committed = true
		log.Printf("orders: inventory handler committed tx eventId=%s orderId=%d", evt.EventID, evt.OrderID)
		return nil
	})

	srv := httpapi.NewServer(orderStore, bus)

	addr := ":" + port
	log.Printf("orders service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, srv.Routes()))
}

func getenv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func is(err error, target error) bool { return errors.Is(err, target) }
