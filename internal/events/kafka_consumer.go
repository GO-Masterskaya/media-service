package events

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type KafkaConsumer struct {
	client  *kgo.Client
	handler func(ctx context.Context, raw []byte) Result
	log     *slog.Logger
	wg      sync.WaitGroup
	closer  io.Closer // optional: e.g. DLQ publisher
}

func NewKafkaConsumer(
	cfg KafkaConsumerConfig,
	handler func(ctx context.Context, raw []byte) Result,
	log *slog.Logger,
) (*KafkaConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka group id required")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &KafkaConsumer{
		client:  client,
		handler: handler,
		log:     log,
	}, nil
}

// SetCloser позволяет зарегистрировать ресурс для graceful shutdown (например, DLQ publisher).
func (c *KafkaConsumer) SetCloser(closer io.Closer) {
	c.closer = closer
}

func (c *KafkaConsumer) Run(ctx context.Context) {
	c.log.Info("kafka consumer started", slog.String("topic", c.getTopic()))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.log.Error("kafka fetch error", slog.Any("error", err))
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			c.wg.Add(1)
			go func(r *kgo.Record) {
				defer c.wg.Done()
				c.processRecord(ctx, r)
			}(record)
		})
	}
}

func (c *KafkaConsumer) processRecord(ctx context.Context, record *kgo.Record) {
	handlerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res := c.handler(handlerCtx, record.Value)

	if !res.Committable {
		c.log.Warn("handler result not committable, skipping offset commit",
			slog.String("event_id", res.EventID.String()),
			slog.Any("error", res.Error),
		)
		select {
		case <-ctx.Done():
		case <-time.After(1 * time.Second):
		}
		return
	}

	if err := c.client.CommitRecords(ctx, record); err != nil {
		c.log.Error("offset commit failed",
			slog.Any("error", err),
			slog.String("event_id", res.EventID.String()),
		)
	}
}

func (c *KafkaConsumer) Shutdown(ctx context.Context) error {
	c.client.Close()

	if c.closer != nil {
		if err := c.closer.Close(); err != nil {
			c.log.Error("closer failed", slog.Any("error", err))
		}
	}

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *KafkaConsumer) getTopic() string {
	topics := c.client.GetConsumeTopics()
	if len(topics) > 0 {
		return topics[0]
	}
	return ""
}
