package kafkabus

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/red-star25/quickbite/orders/internal/dlq"
	"github.com/red-star25/quickbite/orders/internal/failures"
	"github.com/segmentio/kafka-go"
)

const (
	maxAttempts = 8
	maxBackoff  = 5 * time.Second
)

func backoffFor(attempt int) time.Duration {
	d := 200 * time.Millisecond * time.Duration(1<<(attempt-1))
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

func ConsumeInventoryResults(ctx context.Context, r *kafka.Reader, group string, dlqWriter *kafka.Writer, fail *failures.Store, handle func(InventoryResult) error) {
	for { // loop forever until the context is cancelled.
		msg, err := r.FetchMessage(ctx) // fetch the message from the reader.
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("inventory consumer stopped: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var evt InventoryResult
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			// Poison: can never be processed
			_ = dlq.Publish(ctx, dlqWriter, "orders", group, msg, "json unmarshal failed"+err.Error(), 0)
			log.Printf("bad inventory event json: %v", err) // if the is bad log it
			if err := r.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit failed: %v", err)
			} // and commit it anyway so we dont get stuck retrying forever.
			continue // continue to next message
		}

		// Basic validation poison check
		if evt.EventID == "" || evt.OrderID <= 0 || (evt.Type != "InventoryReserved" && evt.Type != "InventoryRejected") {
			_ = dlq.Publish(ctx, dlqWriter, "orders", group, msg, "invalid inventory event fields", 0)
			_ = r.CommitMessages(ctx, msg)
			continue
		}

		log.Printf("orders: inventory event OK: eventId=%s type=%s orderId=%d reserved=%v",
			evt.EventID, evt.Type, evt.OrderID, evt.Reserved)

		// this is where we will do things like, set order status to CONFIRMED or CANCELLED. If returns error, do not commit message. So the same message will be seen again later (retry)
		if err := handle(evt); err != nil {
			attempts, ferr := fail.Inc(ctx, "inventory.v1", msg.Partition, msg.Offset, group, err.Error())
			if ferr != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if attempts >= maxAttempts {
				_ = dlq.Publish(ctx, dlqWriter, "orders", group, msg, "max attempts reached", attempts)
				_ = fail.Clear(ctx, "inventory.v1", msg.Partition, msg.Offset, group)
				_ = r.CommitMessages(ctx, msg)
				continue
			}

			time.Sleep(backoffFor(attempts))
			log.Printf("handle inventory event failed: %v", err)
			// do not commit message so it will be retried later.
			continue
		}

		log.Printf("orders: inventory event handled successfully: eventId=%s orderId=%d", evt.EventID, evt.OrderID)

		_ = fail.Clear(ctx, "inventory.v1", msg.Partition, msg.Offset, group)

		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		} // means -> Kafka, I am done with this message. Dont give it to me again. So you only commit after your DB update
	}
}
