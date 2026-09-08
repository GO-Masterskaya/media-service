package events

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaDLQPublisher отправляет события в dead-letter topic Kafka.
type KafkaDLQPublisher struct {
	client *kgo.Client
	topic  string
}

func NewKafkaDLQPublisher(brokers []string, topic string) (*KafkaDLQPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers required")
	}
	if topic == "" {
		return nil, fmt.Errorf("dlq topic required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &KafkaDLQPublisher{
		client: client,
		topic:  topic,
	}, nil
}

func (p *KafkaDLQPublisher) Publish(ctx context.Context, original []byte, eventID uuid.UUID, reason string) error {
	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(eventID.String()),
		Value: original,
		Headers: []kgo.RecordHeader{
			{Key: "dlq_reason", Value: []byte(reason)},
			{Key: "dlq_timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce to dlq topic %s: %w", p.topic, err)
	}
	return nil
}

func (p *KafkaDLQPublisher) Close() error {
	p.client.Close()
	return nil
}
