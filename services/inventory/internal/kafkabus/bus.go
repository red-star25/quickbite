package kafkabus

import (
	"strings"

	"github.com/segmentio/kafka-go"
)

type Bus struct {
	OrderReader     *kafka.Reader
	InventoryWriter *kafka.Writer
}

func New(brokers []string) *Bus {
	return &Bus{
		OrderReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    "orders.v1",
			GroupID:  "inventory-service",
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		InventoryWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    "inventory.v1",
			Balancer: &kafka.Hash{},
		},
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
	_ = b.OrderReader.Close()
	return b.InventoryWriter.Close()
}
