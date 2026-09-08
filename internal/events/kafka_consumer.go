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
	retries int
}

type KafkaConsumer struct {
	client           kafkaClient
	handler          func(ctx context.Context, raw []byte) Result
	log              *slog.Logger
	wg               sync.WaitGroup
	runWg            sync.WaitGroup
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
						// Не ждём state.done — ребаланс не должен блокироваться
						// на длительной обработке. Воркер сам завершится и удалит
						// себя из map через defer.
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
				// Небуферизованный канал: PollFetches блокируется на ch <- p,
				// пока воркер не заберёт фетч. Это естественный backpressure —
				// новые данные не читаются, пока старые не обработаны.
				state := &partitionWorkerState{
					ch:      make(chan kgo.FetchTopicPartition, 1),
					revoked: make(chan struct{}),
					done:    make(chan struct{}),
				}
				actual, loaded := c.partitionWorkers.LoadOrStore(key, state)
				stateAny = actual
				if !loaded {
					c.wg.Add(1)
					go func(s *partitionWorkerState, k string) {
						defer c.wg.Done()
						c.partitionWorker(ctx, k, s)
					}(state, key)
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
	key string,
	state *partitionWorkerState,
) {
	defer close(state.done)
	defer c.partitionWorkers.Delete(key)

	// Канал с буфером 1: позволяет EachPartition отправить
	// следующий фетч, пока воркер обрабатывает предыдущий.
	// Сохраняет параллельность между партициями и backpressure
	// (третий фетч подряд заблокирует отправку).
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			select {
			case p := <-state.ch:
				c.processPartition(ctx, state, p)
			default:
			}
			return
		case <-state.revoked:
			select {
			case p := <-state.ch:
				c.processPartition(ctx, state, p)
			default:
			}
			return
		case p := <-state.ch:
			c.processPartition(ctx, state, p)
		}
	}
}

func (c *KafkaConsumer) processPartition(ctx context.Context, state *partitionWorkerState, p kgo.FetchTopicPartition) {
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

			backoff := time.Duration(5*(1<<state.retries)) * time.Second
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			state.retries++
			timer := time.NewTimer(backoff)
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
		state.retries = 0
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
