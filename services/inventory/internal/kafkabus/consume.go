package kafkabus

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func ConsumeOrders(ctx context.Context, r *kafka.Reader, handle func(OrderCreated) error) {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			log.Printf("orders consumer stopped: %v", err)
			return
		}

		var evt OrderCreated
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("bad order event json: %v", err)
			if err := r.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit failed: %v", err)
			}
			continue
		}

		if err := handle(evt); err != nil {
			log.Printf("handle order event failed: %v", err)
			continue
		}

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
