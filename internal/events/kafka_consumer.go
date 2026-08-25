package events

import (
	"context"
	"fmt"
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
	client           *kgo.Client
	handler          func(ctx context.Context, raw []byte) Result
	log              *slog.Logger
	wg               sync.WaitGroup // in-flight partition processors
	runWg            sync.WaitGroup // Run() itself
	stopCh           chan struct{}
	stopOnce         sync.Once
	activePartitions sync.Map // key: "topic:partition"
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
	if log == nil {
		log = slog.Default()
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
		stopCh:  make(chan struct{}),
	}, nil
}

func (c *KafkaConsumer) Run(ctx context.Context) {
	c.runWg.Add(1)
	defer c.runWg.Done()

	c.log.Info("kafka consumer started", slog.String("topic", c.getTopic()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
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

		// Параллелим только между партициями; внутри партиции — последовательно.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			key := fmt.Sprintf("%s:%d", p.Topic, p.Partition)
			if _, loaded := c.activePartitions.LoadOrStore(key, struct{}{}); loaded {
				return // партиция уже в обработке, пропускаем этот fetch
			}
			c.wg.Add(1)
			go func(ftp kgo.FetchTopicPartition) {
				defer c.wg.Done()
				defer c.activePartitions.Delete(key)
				c.processPartition(ctx, ftp)
			}(p)
		})
	}
}

func (c *KafkaConsumer) processPartition(ctx context.Context, p kgo.FetchTopicPartition) {
	for _, record := range p.Records {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		handlerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res := c.handler(handlerCtx, record.Value)
		cancel()

		if !res.Committable {
			// Seek на текущий offset — следующий poll начнёт с этой же записи.
			c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				record.Topic: {
					record.Partition: {
						Offset: record.Offset,
						Epoch:  record.LeaderEpoch,
					},
				},
			})

			c.log.Warn("handler result not committable, backing off",
				slog.String("event_id", res.EventID.String()),
				slog.Int64("offset", record.Offset),
				slog.Any("partition", record.Partition),
				slog.Any("error", res.Error),
			)

			// Небольшая задержка, чтобы не спамить Kafka tight loop
			// до тех пор, пока внешняя причина (сеть, БД) не восстановится.
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			case <-c.stopCh:
				timer.Stop()
				return
			}
			return
		}

		if err := c.client.CommitRecords(ctx, record); err != nil {
			c.log.Error("offset commit failed, stopping partition processing",
				slog.Any("error", err),
				slog.Int64("offset", record.Offset),
				slog.Any("partition", record.Partition),
			)
			return
		}
	}
}

func (c *KafkaConsumer) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})

	// Дожидаемся завершения текущих processPartition.
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		c.client.Close()
		return fmt.Errorf("kafka consumer shutdown timeout waiting handlers: %w", ctx.Err())
	}

	// Теперь можно закрывать клиент — in-flight handler'и завершены,
	// новые не начнутся (stopCh закрыт).
	c.client.Close()

	// Дожидаемся выхода Run().
	runDone := make(chan struct{})
	go func() {
		c.runWg.Wait()
		close(runDone)
	}()

	select {
	case <-runDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kafka consumer shutdown timeout waiting Run: %w", ctx.Err())
	}
}

func (c *KafkaConsumer) getTopic() string {
	topics := c.client.GetConsumeTopics()
	if len(topics) > 0 {
		return topics[0]
	}
	return ""
}
