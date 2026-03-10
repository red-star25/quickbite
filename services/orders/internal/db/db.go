package db

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Keeps trying to connect to Postgres until it works, or until it gives up and crashes.
func MustConnectWithRetry(dsn string) *pgxpool.Pool {
	var lastErr error

	// it will try upto 30 times with the sleep of 0.5 second.
	for i := 0; i <= 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		db, err := pgxpool.New(ctx, dsn)
		cancel()

		if err == nil {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel2()

			// if ping succeeds return the pool and you are connected.
			pingErr := db.Ping(ctx2)
			if pingErr == nil {
				return db
			}
			lastErr = pingErr
			db.Close()
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("db connect failed after retries: %v", lastErr)
	return nil
}
