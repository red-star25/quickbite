package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) Begin(ctx context.Context) (pgx.Tx, error) {
	return s.db.Begin(ctx)
}

// returns true if inserted (new), false if already exists
func (s *Store) MarkProcessedEventTx(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events(event_id) VALUES ($1)
		 ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

type Reservation struct {
	OrderID  int64
	Sku      string
	Quantity int
	Reserved bool
	Reason   string
}

func (s *Store) GetReservationTx(ctx context.Context, tx pgx.Tx, orderID int64) (Reservation, bool, error) {
	var r Reservation
	err := tx.QueryRow(ctx,
		`SELECT order_id, sku, quantity, reserved, reason
		 FROM reservations WHERE order_id = $1`,
		orderID,
	).Scan(&r.OrderID, &r.Sku, &r.Quantity, &r.Reserved, &r.Reason)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Reservation{}, false, nil
		}
		return Reservation{}, false, err
	}
	return r, true, nil
}

// Reserve for an order. This is the “durable business operation”.
func (s *Store) ReserveForOrderTx(ctx context.Context, tx pgx.Tx, orderID int64, sku string, qty int) (Reservation, error) {
	// Lock the stock row so concurrent consumers can’t oversell.
	var available int
	err := tx.QueryRow(ctx,
		`SELECT available FROM stock WHERE sku = $1 FOR UPDATE`,
		sku,
	).Scan(&available)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// unknown sku
			return Reservation{
				OrderID:  orderID,
				Sku:      sku,
				Quantity: qty,
				Reserved: false,
				Reason:   "unknown sku",
			}, nil
		}
		return Reservation{}, err
	}

	res := Reservation{
		OrderID:  orderID,
		Sku:      sku,
		Quantity: qty,
	}

	if available < qty {
		res.Reserved = false
		res.Reason = "insufficient stock"
	} else {
		res.Reserved = true
		res.Reason = ""
		_, err = tx.Exec(ctx,
			`UPDATE stock SET available = available - $2, updated_at = now() WHERE sku = $1`,
			sku, qty,
		)
		if err != nil {
			return Reservation{}, err
		}
	}

	// Store the decision (idempotency per order_id)
	_, err = tx.Exec(ctx,
		`INSERT INTO reservations(order_id, sku, quantity, reserved, reason)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (order_id) DO NOTHING`,
		res.OrderID, res.Sku, res.Quantity, res.Reserved, res.Reason,
	)
	if err != nil {
		return Reservation{}, err
	}

	return res, nil
}
