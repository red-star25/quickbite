package kafkabus

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Bus struct {
	// Kafka producer -> uses this to publish messages to topic orders.v1
	OrderWriter *kafka.Writer
	// Kafka consumer -> uses this to read messages from topic inventory.v1
	InventoryReader *kafka.Reader
}

func New(brokers []string) *Bus { // "brokers []string" is a list of broker addresses.
	return &Bus{
		OrderWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...), // tells the writer how to connect to kafka. "brokers..." means spread the brokers
			Topic:    "orders.v1",
			Balancer: &kafka.Hash{},
		},
		InventoryReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,        // same list of brokers as the writer.
			Topic:   "inventory.v1", // reader reads the messages from inventory.v1 topic.
			/*
				Group means, if you run 1 instance of orders service -> it reads all messages.
				If you run 3 instances with the same group ID -> Kafka will split partitions among them.
				GroupID means -> these instances are the same team, split the work.
			*/
			GroupID:  "orders-service",
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

// This function takes brokers from env and pass them to the Kafka client.
func BrokersFromEnv(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (b *Bus) Close() error {
	_ = b.InventoryReader.Close()
	return b.OrderWriter.Close()
}

type OrderCreated struct {
	EventID  string    `json:"eventId"`
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	OrderID  int64     `json:"orderId"`
	UserID   string    `json:"userId"`
	Sku      string    `json:"sku"`
	Quantity int       `json:"quantity"`
}

type InventoryResult struct {
	EventID  string    `json:"eventId"`
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	OrderID  int64     `json:"orderId"`
	Reserved bool      `json:"reserved"`
	Reason   string    `json:"reason,omitempty"`
}

func PublishOrderCreated(ctx context.Context, w *kafka.Writer, evt OrderCreated) error {
	val, err := json.Marshal(evt) // Marshal -> converts the struct into a JSON string.
	if err != nil {
		return err
	}
	// sends one message to Kafka. Key is important as we configured the writer with kafka.Hash{} balancer. Same key -> same partition
	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(evt.OrderID, 10)),
		Value: val,
	})
}
