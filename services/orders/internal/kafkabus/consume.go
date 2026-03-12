package kafkabus

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func ConsumeInventoryResults(ctx context.Context, r *kafka.Reader, handle func(InventoryResult) error) {
	for { // loop forever until the context is cancelled.
		msg, err := r.FetchMessage(ctx) // fetch the message from the reader.
		if err != nil {
			log.Printf("inventory consumer stopped: %v", err)
			return
		}

		var evt InventoryResult
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("bad inventory event json: %v", err) // if the is bad log it
			if err := r.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit failed: %v", err)
			} // and commit it anyway so we dont get stuck retrying forever.
			continue // continue to next message
		}

		// this is where we will do things like, set order status to CONFIRMED or CANCELLED. If returns error, do not commit message. So the same message will be seen again later (retry)
		if err := handle(evt); err != nil {
			log.Printf("handle inventory event failed: %v", err)
			// do not commit message so it will be retried later.
			continue
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		} // means -> Kafka, I am done with this message. Dont give it to me again. So you only commit after your DB update
	}
}
