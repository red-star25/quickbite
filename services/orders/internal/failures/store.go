package failures

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store { return &Store{db: db} }

// Inc increments attempts and returns the new attempt count.
func (s *Store) Inc(ctx context.Context, topic string, partition int, offset int64, groupID string, errMsg string) (int, error) {
	var attempts int
	q := `
INSERT INTO consumer_failures(topic, partition, offset, group_id, attempts, last_error, first_seen, last_seen)
VALUES ($1, $2, $3, $4, 1, $5, now(), now())
ON CONFLICT (topic, partition, offset, group_id)
DO UPDATE SET attempts = consumer_failures.attempts + 1,
              last_error = EXCLUDED.last_error,
              last_seen = now()
RETURNING attempts;
`
	err := s.db.QueryRow(ctx, q, topic, partition, offset, groupID, errMsg).Scan(&attempts)
	return attempts, err
}

func (s *Store) Clear(ctx context.Context, topic string, partition int, offset int64, groupID string) error {
	_, err := s.db.Exec(ctx,
		`DELETE FROM consumer_failures WHERE topic=$1 AND partition=$2 AND offset=$3 AND group_id=$4`,
		topic, partition, offset, groupID,
	)
	return err
}
