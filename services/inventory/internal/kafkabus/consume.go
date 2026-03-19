package kafkabus

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/red-star25/quickbite/inventory/internal/dlq"
	"github.com/red-star25/quickbite/inventory/internal/failures"
	"github.com/segmentio/kafka-go"
)

const (
	maxAttempts = 8
	maxBackoff  = 5 * time.Second
	sourceTopic = "orders.v1"
)

func backoffFor(attempt int) time.Duration {
	d := 200 * time.Millisecond * time.Duration(1<<(attempt-1))
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

func ConsumeOrderCreated(ctx context.Context, bus *Bus, fail *failures.Store, handle func(OrderCreated) error) {
	r := bus.OrdersReader
	group := bus.ConsumerGroup
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("inventory consumer stopped: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var evt OrderCreated
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			_ = dlq.Publish(ctx, bus.DLQWriter, "inventory", group, msg, "json unmarshal failed"+err.Error(), 0)
			log.Printf("bad inventory event json: %v", err)
			if err := r.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit failed: %v", err)
			}
			continue
		}

		if evt.Type != "OrderCreated" || evt.EventID == "" || evt.OrderID <= 0 || strings.TrimSpace(evt.Sku) == "" || evt.Quantity <= 0 {
			_ = dlq.Publish(ctx, bus.DLQWriter, "inventory", group, msg, "invalid inventory event fields", 0)
			_ = r.CommitMessages(ctx, msg)
			continue
		}

		if err := handle(evt); err != nil {
			attempts, ferr := fail.Inc(ctx, msg.Topic, msg.Partition, msg.Offset, group, err.Error())
			if ferr != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if attempts >= maxAttempts {
				_ = dlq.Publish(ctx, bus.DLQWriter, "inventory", group, msg, "max attempts reached", attempts)
				_ = fail.Clear(ctx, msg.Topic, msg.Partition, msg.Offset, group)
				_ = r.CommitMessages(ctx, msg)
				continue
			}

			time.Sleep(backoffFor(attempts))
			log.Printf("handle inventory event failed: %v", err)
			continue
		}

		_ = fail.Clear(ctx, msg.Topic, msg.Partition, msg.Offset, group)

		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}

func PublishInventoryResult(ctx context.Context, w *kafka.Writer, evt InventoryResult) error {
	val, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(evt.OrderID, 10)),
		Value: val,
	})
}
