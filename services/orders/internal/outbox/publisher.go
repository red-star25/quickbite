package outbox

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	DB     *pgxpool.Pool
	Writer *kafka.Writer
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		log.Printf("outbox begin tx failed: %v", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	events, err := FetchUnpublishedForUpdate(ctx, tx, 25)
	if err != nil {
		log.Printf("outbox fetch failed: %v", err)
		return
	}
	if len(events) == 0 {
		_ = tx.Commit(ctx)
		return
	}

	for _, e := range events {
		err := p.Writer.WriteMessages(ctx, kafka.Message{
			Topic: e.Topic,
			Key:   []byte(e.Key),
			Value: e.Payload,
		})
		if err != nil {
			log.Printf("outbox write message failed: %v", err)
			return
		}
		if err := MarkPublishedTx(ctx, tx, e.ID); err != nil {
			log.Printf("outbox mark published failed: %v", err)
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("outbox commit failed: %v", err)
		return
	}
}
