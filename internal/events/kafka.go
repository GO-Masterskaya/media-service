package events

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type kafkaClient struct { client *kgo.Client; dlqTopic string }

func newKafkaClient(cfg Config) (client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
	}
	if cfg.Username != "" { opts = append(opts, kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism())) }
	c, err := kgo.NewClient(opts...)
	if err != nil { return nil, err }
	return &kafkaClient{client: c, dlqTopic: cfg.DLQTopic}, nil
}

func (c *kafkaClient) Poll(ctx context.Context) ([]Record, error) {
	fetches := c.client.PollFetches(ctx)
	if err := fetches.Err(); err != nil { return nil, err }
	records := make([]Record, 0)
	fetches.EachRecord(func(r *kgo.Record) {
		headers := make(map[string]string, len(r.Headers))
		for _, h := range r.Headers { headers[h.Key] = string(h.Value) }
		records = append(records, Record{Topic:r.Topic, Key:r.Key, Value:r.Value, Headers:headers, Partition:r.Partition, Offset:r.Offset})
	})
	return records, nil
}

func (c *kafkaClient) Commit(ctx context.Context, record Record) error {
	return c.client.CommitRecords(ctx, &kgo.Record{Topic:record.Topic, Partition:record.Partition, Offset:record.Offset})
}

func (c *kafkaClient) ProduceDLQ(ctx context.Context, record Record, reason string) error {
	headers := make([]kgo.RecordHeader, 0, len(record.Headers)+1)
	for key, value := range record.Headers { headers = append(headers, kgo.RecordHeader{Key:key, Value:[]byte(value)}) }
	headers = append(headers, kgo.RecordHeader{Key:"x-dlq-reason", Value:[]byte(reason)})
	result := c.client.ProduceSync(ctx, &kgo.Record{Topic:c.dlqTopic, Key:record.Key, Value:record.Value, Headers:headers})
	if result.FirstErr() != nil { return fmt.Errorf("produce DLQ: %w", result.FirstErr()) }
	return nil
}

func (c *kafkaClient) Close() { c.client.Close() }
