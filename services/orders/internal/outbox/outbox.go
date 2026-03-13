package outbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type Event struct {
	ID      int64
	EventID string
	Topic   string
	Key     string
	Payload []byte
}

func InsertTx(ctx context.Context, tx pgx.Tx, evt Event) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (event_id, topic, key, payload) VALUES ($1, $2, $3, $4)`,
		evt.EventID, evt.Topic, evt.Key, evt.Payload,
	)
	return err
}

func FetchUnpublishedForUpdate(ctx context.Context, tx pgx.Tx, limit int) ([]Event, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, event_id, topic, key, payload 
		FROM outbox_events 
		WHERE published_at IS NULL 
		ORDER BY id ASC 
		LIMIT $1 
		FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.EventID, &e.Topic, &e.Key, &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func MarkPublishedTx(ctx context.Context, tx pgx.Tx, id int64) error {
	_, err := tx.Exec(ctx,
		`UPDATE outbox_events SET published_at = $2 WHERE id = $1`,
		id, time.Now().UTC(),
	)
	return err
}
