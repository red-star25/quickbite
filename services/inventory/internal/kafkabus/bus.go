package kafkabus

import (
	"strings"

	"github.com/segmentio/kafka-go"
)

type Bus struct {
	OrdersReader    *kafka.Reader
	InventoryWriter *kafka.Writer
	DLQWriter       *kafka.Writer
	ConsumerGroup   string
}

func New(brokers []string) *Bus {
	group := "inventory-service"
	return &Bus{
		OrdersReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    "orders.v1",
			GroupID:  group,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		InventoryWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "inventory.v1",
			Balancer: &kafka.Hash{},
		},
		DLQWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "inventory.dlq.v1",
			Balancer: &kafka.Hash{},
		},
		ConsumerGroup: group,
	}
}

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
	_ = b.OrdersReader.Close()
	_ = b.DLQWriter.Close()
	return b.InventoryWriter.Close()
}
