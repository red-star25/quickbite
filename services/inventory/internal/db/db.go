package db

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func MustConnectWithRetry(dsn string) *pgxpool.Pool {
	var lastErr error

	for i := 1; i <= 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pool, err := pgxpool.New(ctx, dsn)
		cancel()

		if err == nil {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			pingErr := pool.Ping(ctx2)
			cancel2()

			if pingErr == nil {
				return pool
			}

			lastErr = pingErr
			pool.Close()
		} else {
			lastErr = err
		}

		time.Sleep(500 * time.Millisecond)
	}

	log.Fatalf("inventory db connect failed after retries: %v", lastErr)
	return nil
}
