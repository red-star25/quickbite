package dlq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type Header struct {
	Key   string `json:"key"`
	Value string `json:"valueB64"`
}

type DeadLetter struct {
	DLQVersion      int       `json:"dlqVersion"`
	Service         string    `json:"service"`
	ConsumerGroup   string    `json:"consumerGroup"`
	SourceTopic     string    `json:"sourceTopic"`
	SourcePartition int       `json:"sourcePartition"`
	SourceOffset    int64     `json:"sourceOffset"`
	SourceTime      time.Time `json:"sourceTime"`
	KeyB64          string    `json:"keyB64"`
	PayloadB64      string    `json:"payloadB64"`
	Headers         []Header  `json:"headers"`
	Error           string    `json:"error"`
	Attempts        int       `json:"attempts"`
}

func Publish(ctx context.Context, w *kafka.Writer, service string, group string, msg kafka.Message, reason string, attempts int) error {
	hdrs := make([]Header, 0, len(msg.Headers))
	for _, h := range msg.Headers {
		hdrs = append(hdrs, Header{
			Key:   h.Key,
			Value: base64.StdEncoding.EncodeToString(h.Value),
		})
	}

	d := DeadLetter{
		DLQVersion:      1,
		Service:         service,
		ConsumerGroup:   group,
		SourceTopic:     msg.Topic,
		SourcePartition: msg.Partition,
		SourceOffset:    msg.Offset,
		SourceTime:      msg.Time,
		KeyB64:          base64.StdEncoding.EncodeToString(msg.Key),
		PayloadB64:      base64.StdEncoding.EncodeToString(msg.Value),
		Headers:         hdrs,
		Error:           reason,
		Attempts:        attempts,
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	return w.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: b,
	})
}
