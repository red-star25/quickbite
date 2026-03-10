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
	Status    string
	Sku       string
	Quantity  int
	CreatedAt time.Time
}

const (
	StatusPending   = "PENDING"
	StatusConfirmed = "CONFIRMED"
	StatusCancelled = "CANCELLED"
)

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

func (s *OrdersStore) Create(ctx context.Context, userID, note, sku string, quantity int) (int64, error) {
	var id int64
	err := s.db.QueryRow(ctx,
		`INSERT INTO orders (user_id, note, status, sku, quantity) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		userID, note,
		StatusPending,
		sku,
		quantity,
	).Scan(&id)
	return id, err
}

func (s *OrdersStore) UpdateStatus(ctx context.Context, id int64, status string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE orders SET status = $1 WHERE id = $2`,
		status,
		id,
	)
	return err
}

func (s *OrdersStore) GetByID(ctx context.Context, id int64) (Order, error) {
	var o Order
	err := s.db.QueryRow(ctx,
		`SELECT id, user_id, note, status, sku, quantity, created_at FROM orders WHERE id = $1`,
		id,
	).Scan(&o.ID, &o.UserID, &o.Note, &o.Status, &o.Sku, &o.Quantity, &o.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Order{}, pgx.ErrNoRows
		}
		return Order{}, err
	}
	return o, nil
}
