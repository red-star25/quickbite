package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"quickbite/orders/internal/db"
	httpapi "quickbite/orders/internal/http"
	"quickbite/orders/internal/store"
	"strings"
)

func main() {
	// Read config from env
	// try to read port from env, if not set, use 8080
	port := getenv("PORT", "8080")
	// dsn - Data Source Name. Its a database connection string.
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable")

	/*
		Connects to Postgres.
		"With retry" means it keepstrying for a bit if the DB isn't ready yet.
		"must" means if it fails, the program exists/crashes
	*/
	pool := db.MustConnectWithRetry(dsn)
	defer pool.Close()

	orderStore := store.NewOrdersStore(pool)
	srv := httpapi.NewServer(orderStore)

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
