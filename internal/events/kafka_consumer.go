package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaClient — абстракция над kgo.Client для тестируемости.
type kafkaClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	SetOffsets(offsets map[string]map[int32]kgo.EpochOffset)
	PauseFetchPartitions(partitions map[string][]int32) map[string][]int32
	ResumeFetchPartitions(partitions map[string][]int32)
	Close()
	GetConsumeTopics() []string
}

type KafkaConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type partitionWorkerState struct {
	ch      chan kgo.FetchTopicPartition
	revoked chan struct{}
	done    chan struct{}
}

type KafkaConsumer struct {
	client           kafkaClient
	handler          func(ctx context.Context, raw []byte) Result
	log              *slog.Logger
	wg               sync.WaitGroup // partition workers
	runWg            sync.WaitGroup // Run() itself — ждём перед wg.Wait()
	stopCh           chan struct{}
	stopOnce         sync.Once
	partitionWorkers sync.Map
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

	c := &KafkaConsumer{
		handler: handler,
		log:     log,
		stopCh:  make(chan struct{}),
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			for topic, partitions := range revoked {
				for _, partition := range partitions {
					key := fmt.Sprintf("%s:%d", topic, partition)
					if val, ok := c.partitionWorkers.Load(key); ok {
						state := val.(*partitionWorkerState)
						close(state.revoked)
						<-state.done
						c.partitionWorkers.Delete(key)
					}
				}
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	c.client = client
	return c, nil
}

func (c *KafkaConsumer) Run(ctx context.Context) error {
	c.runWg.Add(1)
	defer c.runWg.Done()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.log.Error("fetch error", "topic", err.Topic, "partition", err.Partition, "err", err.Err)
			}
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			key := fmt.Sprintf("%s:%d", p.Topic, p.Partition)

			stateAny, loaded := c.partitionWorkers.Load(key)
			if !loaded {
				state := &partitionWorkerState{
					ch:      make(chan kgo.FetchTopicPartition),
					revoked: make(chan struct{}),
					done:    make(chan struct{}),
				}
				actual, loaded := c.partitionWorkers.LoadOrStore(key, state)
				stateAny = actual
				if !loaded {
					c.wg.Add(1)
					go func(s *partitionWorkerState, topic string, partition int32) {
						defer c.wg.Done()
						c.partitionWorker(ctx, topic, partition, s)
					}(state, p.Topic, p.Partition)
				}
			}

			state := stateAny.(*partitionWorkerState)
			select {
			case state.ch <- p:
			case <-state.revoked:
			case <-ctx.Done():
			case <-c.stopCh:
			}
		})
	}
}

func (c *KafkaConsumer) partitionWorker(
	ctx context.Context,
	topic string,
	partition int32,
	state *partitionWorkerState,
) {
	defer close(state.done)

	c.client.PauseFetchPartitions(map[string][]int32{topic: {partition}})
	defer func() {
		c.client.ResumeFetchPartitions(map[string][]int32{topic: {partition}})
		c.log.Debug("resumed partition", "topic", topic, "partition", partition)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			select {
			case p := <-state.ch:
				c.processPartition(ctx, p)
			default:
			}
			return
		case <-state.revoked:
			select {
			case p := <-state.ch:
				c.processPartition(ctx, p)
			default:
			}
			return
		case p := <-state.ch:
			c.processPartition(ctx, p)
		}
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

// Shutdown последовательность:
//  1. close(stopCh) — сигнал воркерам.
//  2. client.Close() — прерывает PollFetches, Run() выходит из цикла.
//  3. runWg.Wait() — ждём, пока Run() завершится (больше не будет wg.Add).
//  4. wg.Wait() — ждём завершения всех воркеров.
func (c *KafkaConsumer) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})

	c.client.Close()

	// Шаг 1: Run() должен завершиться — после этого новые wg.Add невозможны.
	runDone := make(chan struct{})
	go func() {
		c.runWg.Wait()
		close(runDone)
	}()

	select {
	case <-runDone:
	case <-ctx.Done():
		return fmt.Errorf("kafka consumer shutdown timeout waiting Run: %w", ctx.Err())
	}

	// Шаг 2: теперь безопасно ждать воркеров.
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kafka consumer shutdown timeout waiting handlers: %w", ctx.Err())
	}
}

func (c *KafkaConsumer) getTopic() string {
	topics := c.client.GetConsumeTopics()
	if len(topics) > 0 {
		return topics[0]
	}
	return ""
}
