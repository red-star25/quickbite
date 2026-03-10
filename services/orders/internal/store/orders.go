package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Order struct {
	ID        int64
	UserID    string
	Note      string
	CreatedAt time.Time
}

/*
OrderStore is the main store for the orders service.
*/
type OrdersStore struct {
	db *pgxpool.Pool
}

// This is a constructor function that returns a pointer to a new OrdersStore.
func NewOrdersStore(db *pgxpool.Pool) *OrdersStore {
	return &OrdersStore{db: db}
}

func (s *OrdersStore) Create(ctx context.Context, userID, note string) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx,
		`INSERT INTO orders (user_id, note) VALUES ($1, $2) RETURNING id`,
		userID, note,
	).Scan(&id)
	return id, err
}

func (s *OrdersStore) GetByID(ctx context.Context, id int64) (Order, error) {
	var o Order
	err := s.db.QueryRow(ctx,
		`SELECT id, user_id, note, created_at FROM orders WHERE id = $1`,
		id,
	).Scan(&o.ID, &o.UserID, &o.Note, &o.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Order{}, err
		}
		return Order{}, err
	}
	return o, nil
}
