package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/red-star25/quickbite/orders/internal/db"
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

	go kafkabus.ConsumeInventoryResults(ctx, bus.InventoryReader, func(evt kafkabus.InventoryResult) error {
		tx, err := orderStore.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		newEvt, err := orderStore.MarkProcessedEventTx(ctx, tx, evt.EventID)
		if err != nil {
			return err
		}
		if !newEvt {
			return nil
		}

		switch evt.Type {
		case "InventoryReserved":
			return orderStore.UpdateStatusTx(ctx, tx, evt.OrderID, store.StatusConfirmed)
		case "InventoryRejected":
			return orderStore.UpdateStatusTx(ctx, tx, evt.OrderID, store.StatusCancelled)
		default:
			return tx.Commit(ctx)
		}
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
