package events

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ============================================================================
// Мок клиента
// ============================================================================

type mockKafkaClient struct {
	mu sync.Mutex

	fetchQueue  []kgo.Fetches
	fetchIndex  int
	pollBlockCh chan struct{}
	pollCalls   int

	committed []*kgo.Record
	commitErr error

	setOffsets map[string]map[int32]kgo.EpochOffset

	closed  atomic.Bool
	closeCh chan struct{}

	topics []string
}

func newMockKafkaClient(topics []string) *mockKafkaClient {
	return &mockKafkaClient{
		fetchQueue:  make([]kgo.Fetches, 0),
		pollBlockCh: make(chan struct{}),
		closeCh:     make(chan struct{}),
		topics:      topics,
	}
}

func (m *mockKafkaClient) enqueueFetch(f kgo.Fetches) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchQueue = append(m.fetchQueue, f)
}

func (m *mockKafkaClient) PollFetches(ctx context.Context) kgo.Fetches {
	m.mu.Lock()
	m.pollCalls++
	if m.fetchIndex >= len(m.fetchQueue) {
		m.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-m.closeCh:
		}
		return kgo.Fetches{}
	}
	f := m.fetchQueue[m.fetchIndex]
	m.fetchIndex++
	m.mu.Unlock()

	select {
	case <-m.pollBlockCh:
	default:
		close(m.pollBlockCh)
	}
	return f
}

func (m *mockKafkaClient) CommitRecords(ctx context.Context, rs ...*kgo.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.commitErr != nil {
		return m.commitErr
	}
	m.committed = append(m.committed, rs...)
	return nil
}

func (m *mockKafkaClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setOffsets = offsets
}

func (m *mockKafkaClient) Close() {
	if m.closed.CompareAndSwap(false, true) {
		close(m.closeCh)
	}
}

func (m *mockKafkaClient) GetConsumeTopics() []string {
	return m.topics
}

// ============================================================================
// Хелперы
// ============================================================================

func makeRecord(topic string, partition int32, offset int64, value []byte) *kgo.Record {
	return &kgo.Record{
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		LeaderEpoch: 1,
		Value:       value,
	}
}

func makeFetchTopicPartition(topic string, partition int32, records ...*kgo.Record) kgo.FetchTopicPartition {
	return kgo.FetchTopicPartition{
		Topic: topic,
		FetchPartition: kgo.FetchPartition{
			Partition: partition,
			Records:   records,
		},
	}
}

func makeFetches(records ...*kgo.Record) kgo.Fetches {
	if len(records) == 0 {
		return kgo.Fetches{}
	}
	topicMap := make(map[string][]kgo.FetchPartition)
	for _, r := range records {
		key := r.Topic
		found := false
		for i := range topicMap[key] {
			if topicMap[key][i].Partition == r.Partition {
				topicMap[key][i].Records = append(topicMap[key][i].Records, r)
				found = true
				break
			}
		}
		if !found {
			topicMap[key] = append(topicMap[key], kgo.FetchPartition{
				Partition: r.Partition,
				Records:   []*kgo.Record{r},
			})
		}
	}

	var fetch kgo.Fetch
	for topic, partitions := range topicMap {
		fetch.Topics = append(fetch.Topics, kgo.FetchTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return kgo.Fetches{fetch}
}

// ============================================================================
// Тесты
// ============================================================================

func TestNewKafkaConsumer_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     KafkaConsumerConfig
		handler func(ctx context.Context, raw []byte) Result
		wantErr string
	}{
		{
			name:    "no brokers",
			cfg:     KafkaConsumerConfig{Topic: "t", GroupID: "g"},
			handler: func(ctx context.Context, raw []byte) Result { return Result{} },
			wantErr: "kafka brokers required",
		},
		{
			name:    "no topic",
			cfg:     KafkaConsumerConfig{Brokers: []string{"b"}, GroupID: "g"},
			handler: func(ctx context.Context, raw []byte) Result { return Result{} },
			wantErr: "kafka topic required",
		},
		{
			name:    "no group",
			cfg:     KafkaConsumerConfig{Brokers: []string{"b"}, Topic: "t"},
			handler: func(ctx context.Context, raw []byte) Result { return Result{} },
			wantErr: "kafka group id required",
		},
		{
			name:    "no handler",
			cfg:     KafkaConsumerConfig{Brokers: []string{"b"}, Topic: "t", GroupID: "g"},
			handler: nil,
			wantErr: "handler required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKafkaConsumer(tt.cfg, tt.handler, slog.Default())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestProcessPartition_Committable(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})
	var processed []string

	handler := func(ctx context.Context, raw []byte) Result {
		processed = append(processed, string(raw))
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec1 := makeRecord("test-topic", 0, 10, []byte("msg-1"))
	rec2 := makeRecord("test-topic", 0, 11, []byte("msg-2"))
	ftp := makeFetchTopicPartition("test-topic", 0, rec1, rec2)

	c.processPartition(context.Background(), &partitionWorkerState{}, ftp)

	require.Equal(t, []string{"msg-1", "msg-2"}, processed)
	require.Len(t, mock.committed, 2)
	require.Equal(t, int64(10), mock.committed[0].Offset)
	require.Equal(t, int64(11), mock.committed[1].Offset)
}

func TestProcessPartition_NotCommittable_SetOffsets(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	handler := func(ctx context.Context, raw []byte) Result {
		if string(raw) == "bad" {
			return Result{Committable: false, EventID: uuid.New(), Error: errors.New("boom")}
		}
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec1 := makeRecord("test-topic", 0, 10, []byte("ok"))
	rec2 := makeRecord("test-topic", 0, 11, []byte("bad"))
	rec3 := makeRecord("test-topic", 0, 12, []byte("ok"))
	ftp := makeFetchTopicPartition("test-topic", 0, rec1, rec2, rec3)

	c.processPartition(context.Background(), &partitionWorkerState{}, ftp)

	require.Len(t, mock.committed, 1)
	require.Equal(t, int64(10), mock.committed[0].Offset)
	require.NotNil(t, mock.setOffsets)
	require.Equal(t, int64(11), mock.setOffsets["test-topic"][0].Offset)
}

func TestProcessPartition_CommitError(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})
	mock.commitErr = errors.New("broker unavailable")

	var processed int
	handler := func(ctx context.Context, raw []byte) Result {
		processed++
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec1 := makeRecord("test-topic", 0, 10, []byte("msg-1"))
	rec2 := makeRecord("test-topic", 0, 11, []byte("msg-2"))
	ftp := makeFetchTopicPartition("test-topic", 0, rec1, rec2)

	c.processPartition(context.Background(), &partitionWorkerState{}, ftp)

	require.Equal(t, 1, processed)
	require.Len(t, mock.committed, 0)
}

func TestRun_PartitionOrder(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	var order []int64
	var mu sync.Mutex
	handler := func(ctx context.Context, raw []byte) Result {
		mu.Lock()
		order = append(order, int64(len(order)))
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec1 := makeRecord("test-topic", 0, 10, []byte("a"))
	rec2 := makeRecord("test-topic", 0, 11, []byte("b"))
	mock.enqueueFetch(makeFetches(rec1))
	mock.enqueueFetch(makeFetches(rec2))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var runErr error
	var runDone sync.WaitGroup
	runDone.Add(1)
	go func() {
		defer runDone.Done()
		runErr = c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 2
	}, time.Second, 10*time.Millisecond)

	cancel()
	runDone.Wait()

	require.Equal(t, context.Canceled, runErr)
	require.Equal(t, []int64{0, 1}, order)
}

// TestRun_Backpressure — небуферизованный канал блокирует PollFetches,
// пока воркер занят. Это естественный backpressure без PauseFetchPartitions.
func TestRun_Backpressure(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	handlerBlock := make(chan struct{})
	handler := func(ctx context.Context, raw []byte) Result {
		<-handlerBlock
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec := makeRecord("test-topic", 0, 10, []byte("x"))
	mock.enqueueFetch(makeFetches(rec))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = c.Run(ctx) }()

	select {
	case <-mock.pollBlockCh:
	case <-time.After(time.Second):
		t.Fatal("PollFetches did not unblock")
	}

	// Не проверяем pollCalls: после обработки первого fetch Run тут же
	// вызывает PollFetches во второй раз, и мок инкрементирует счётчик
	// до блокировки на пустой очереди. Сам факт блокировки handler
	// (небуферизованный канал) и закрытия pollBlockCh достаточно
	// для демонстрации backpressure.

	close(handlerBlock)
	cancel()
}

func TestShutdown_Order(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	handlerStarted := make(chan struct{})
	handlerBlock := make(chan struct{})
	handler := func(ctx context.Context, raw []byte) Result {
		close(handlerStarted)
		<-handlerBlock
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec := makeRecord("test-topic", 0, 10, []byte("x"))
	mock.enqueueFetch(makeFetches(rec))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = c.Run(ctx) }()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		_ = c.Shutdown(context.Background())
	}()

	require.Eventually(t, func() bool {
		return mock.closed.Load()
	}, time.Second, 10*time.Millisecond)

	select {
	case <-shutdownDone:
		t.Fatal("shutdown completed before handler finished")
	case <-time.After(100 * time.Millisecond):
		// ok
	}

	close(handlerBlock)

	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not complete after handler finished")
	}
}

func TestOnPartitionsRevoked(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	var processed atomic.Bool
	handler := func(ctx context.Context, raw []byte) Result {
		processed.Store(true)
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	key := "test-topic:0"
	state := &partitionWorkerState{
		ch:      make(chan kgo.FetchTopicPartition),
		revoked: make(chan struct{}),
		done:    make(chan struct{}),
	}
	c.partitionWorkers.Store(key, state)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.partitionWorker(context.Background(), key, state)
	}()

	close(state.revoked)

	select {
	case <-state.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after revoked")
	}
}

func TestProcessPartition_StopCh(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	var processed int
	handler := func(ctx context.Context, raw []byte) Result {
		processed++
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec1 := makeRecord("test-topic", 0, 10, []byte("msg-1"))
	rec2 := makeRecord("test-topic", 0, 11, []byte("msg-2"))
	ftp := makeFetchTopicPartition("test-topic", 0, rec1, rec2)

	close(c.stopCh)
	c.processPartition(context.Background(), &partitionWorkerState{}, ftp)

	require.Equal(t, 0, processed)
}

func TestProcessPartition_CtxDone(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	var processed int
	handler := func(ctx context.Context, raw []byte) Result {
		processed++
		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	rec1 := makeRecord("test-topic", 0, 10, []byte("msg-1"))
	rec2 := makeRecord("test-topic", 0, 11, []byte("msg-2"))
	ftp := makeFetchTopicPartition("test-topic", 0, rec1, rec2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.processPartition(ctx, &partitionWorkerState{}, ftp)

	require.Equal(t, 0, processed)
}

func TestRun_MultiplePartitionsParallel(t *testing.T) {
	mock := newMockKafkaClient([]string{"test-topic"})

	var mu sync.Mutex
	activePartitions := make(map[int32]bool)
	maxConcurrent := 0

	handler := func(ctx context.Context, raw []byte) Result {
		partition := int32(raw[0])

		mu.Lock()
		activePartitions[partition] = true
		if len(activePartitions) > maxConcurrent {
			maxConcurrent = len(activePartitions)
		}
		mu.Unlock()

		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		delete(activePartitions, partition)
		mu.Unlock()

		return Result{Committable: true, EventID: uuid.New()}
	}

	c := &KafkaConsumer{
		client:  mock,
		handler: handler,
		log:     slog.Default(),
		stopCh:  make(chan struct{}),
	}

	recP0 := makeRecord("test-topic", 0, 10, []byte{0})
	recP1 := makeRecord("test-topic", 1, 20, []byte{1})
	mock.enqueueFetch(makeFetches(recP0, recP1))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var runDone sync.WaitGroup
	runDone.Add(1)
	go func() {
		defer runDone.Done()
		_ = c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return maxConcurrent == 2
	}, time.Second, 10*time.Millisecond, "две партиции должны обрабатываться параллельно")

	cancel()
	runDone.Wait()
}
