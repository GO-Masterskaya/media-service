package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// defaultPollTimeout — потолок ожидания одного PollFetches, если
	// KafkaConsumerConfig.PollTimeout не задан.
	defaultPollTimeout = time.Second
	// reconnectBackoffBase — стартовая пауза после ошибки poll. Дальше
	// удваивается до reconnectMaxBackoff и сбрасывается на первом
	// успешном poll.
	reconnectBackoffBase = 100 * time.Millisecond
	// defaultReconnectMaxBackoff — потолок паузы, если
	// KafkaConsumerConfig.ReconnectMaxBackoff не задан.
	defaultReconnectMaxBackoff = 10 * time.Second
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
	// Security — TLS и SASL/SCRAM. Нулевое значение = PLAINTEXT без
	// авторизации, это допустимо только для локального compose.
	Security KafkaSecurity
	// PollTimeout — потолок ожидания одного PollFetches. 0 → defaultPollTimeout.
	PollTimeout time.Duration
	// ReconnectMaxBackoff — потолок паузы между повторами после ошибки poll.
	// 0 → defaultReconnectMaxBackoff.
	ReconnectMaxBackoff time.Duration
	// LogLevel — уровень логов самого franz-go: none/error/warn/info/debug.
	// Пустая строка → DefaultKafkaLogLevel.
	LogLevel string
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

	// Нормализованные значения из KafkaConsumerConfig. Хранятся здесь,
	// а не в cfg, чтобы Run не зависел от конструктора: часть unit-тестов
	// собирает KafkaConsumer литералом.
	pollTimeout         time.Duration
	reconnectMaxBackoff time.Duration
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
	if err := cfg.Security.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = defaultPollTimeout
	}
	if cfg.ReconnectMaxBackoff <= 0 {
		cfg.ReconnectMaxBackoff = defaultReconnectMaxBackoff
	}

	c := &KafkaConsumer{
		handler:             handler,
		log:                 log,
		stopCh:              make(chan struct{}),
		pollTimeout:         cfg.PollTimeout,
		reconnectMaxBackoff: cfg.ReconnectMaxBackoff,
	}

	opts := []kgo.Opt{
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
	}
	// TLS и SASL добавляются последними и только если заданы: пустая
	// Security оставляет клиента в PLAINTEXT для локального compose.
	opts = append(opts, cfg.Security.clientOpts()...)

	// Без этой опции franz-go пишет в no-op логгер, и недоступность
	// брокера до входа в группу остаётся полностью незаметной (#80).
	logOpt, err := kafkaLoggerOpt(log, cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	opts = append(opts, logOpt)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	c.client = client
	return c, nil
}

func (c *KafkaConsumer) Run(ctx context.Context) error {
	c.runWg.Add(1)
	defer c.runWg.Done()

	// Потолок берём из поля, но не доверяем ему вслепую: литерал в тесте
	// может оставить ноль, и тогда удвоение backoff стало бы бесконечным.
	maxBackoff := c.reconnectMaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultReconnectMaxBackoff
	}
	backoff := min(reconnectBackoffBase, maxBackoff)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		default:
		}

		fetches := c.pollOnce(ctx)

		// Err0 — дешёвая проверка «искусственного» фетча. franz-go
		// подставляет одиночный фетч с topic="" и partition=-1, когда
		// poll прерван: закрытием клиента, отменой или дедлайном контекста.
		// Записей в таком фетче нет никогда (см. PollRecords), поэтому
		// выходить/продолжать здесь безопасно.
		if err := fetches.Err0(); err != nil {
			switch {
			case errors.Is(err, kgo.ErrClientClosed):
				return nil
			case errors.Is(err, context.Canceled):
				return ctx.Err()
			case errors.Is(err, context.DeadlineExceeded):
				// Истёк дедлайн одного poll: в топике просто нет новых
				// записей. Это не обрыв связи — backoff не трогаем
				// и сразу поллим снова.
				continue
			}
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.log.Error("fetch error", "topic", err.Topic, "partition", err.Partition, "err", err.Err)
			}
			// Ошибки есть, записей нет — считаем это недоступностью
			// брокера. Без паузы цикл превращается в busy-loop: poll
			// падает мгновенно и мы сразу зовём его снова.
			if fetches.NumRecords() == 0 {
				c.log.Warn("kafka poll failed, backing off", slog.Duration("backoff", backoff))
				if !c.sleep(ctx, backoff) {
					// Прервано ctx или stopCh — выйдем через select
					// в начале следующей итерации.
					continue
				}
				backoff = nextBackoff(backoff, maxBackoff)
				continue
			}
		}
		// Данные пришли — соединение живо, начинаем отсчёт заново.
		backoff = min(reconnectBackoffBase, maxBackoff)

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			// Партиция без записей воркера не заслуживает: он бы навсегда
			// осел в partitionWorkers и жил до shutdown. Сюда попадают
			// в том числе «искусственные» партиции -1 с ошибкой.
			if len(p.Records) == 0 {
				return
			}

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

// pollOnce ограничивает время одного PollFetches. Без дедлайна poll висит
// до появления записей, и цикл Run не может проверить stopCh — остановка
// держится только на client.Close(). С дедлайном цикл дышит раз в
// pollTimeout, а истечение дедлайна отличимо от ошибки связи.
//
// Дочерний контекст обязателен: отмена родительского ctx через него
// проходит, а обратно дедлайн не протекает.
func (c *KafkaConsumer) pollOnce(ctx context.Context) kgo.Fetches {
	if c.pollTimeout <= 0 {
		return c.client.PollFetches(ctx)
	}
	pollCtx, cancel := context.WithTimeout(ctx, c.pollTimeout)
	defer cancel()
	return c.client.PollFetches(pollCtx)
}

// sleep возвращает false, если пауза прервана остановкой консьюмера.
// Обычный time.Sleep здесь недопустим: он задержал бы shutdown на весь
// backoff.
func (c *KafkaConsumer) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-c.stopCh:
		return false
	}
}

// nextBackoff удваивает паузу, не превышая потолок.
// Параметр назван limit, а не max, чтобы не перекрывать встроенный max.
func nextBackoff(current, limit time.Duration) time.Duration {
	next := current * 2
	if next > limit {
		return limit
	}
	return next
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
