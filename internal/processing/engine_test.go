package processing_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/processing"
)

// MockJobRepository — потокобезопасный мок репозитория для unit-тестов.
type MockJobRepository struct {
	mu           sync.Mutex
	queuedJobs   []processing.Job
	maxClaimedAt int
}

func NewMockJobRepository(jobs []processing.Job) *MockJobRepository {
	return &MockJobRepository{
		queuedJobs: jobs,
	}
}

func (m *MockJobRepository) ClaimQueued(ctx context.Context, limit int) ([]processing.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit > m.maxClaimedAt {
		m.maxClaimedAt = limit
	}

	if len(m.queuedJobs) == 0 {
		return nil, nil
	}

	count := limit
	if count > len(m.queuedJobs) {
		count = len(m.queuedJobs)
	}

	claimed := make([]processing.Job, count)
	copy(claimed, m.queuedJobs[:count])
	m.queuedJobs = m.queuedJobs[count:]

	return claimed, nil
}

func (m *MockJobRepository) GetQueueDepth(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.queuedJobs)), nil
}

func (m *MockJobRepository) GetMaxClaimedLimit() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxClaimedAt
}

// 1. Тест: Одновременно работает не больше WORKER_CONCURRENCY jobs
func TestWorkerConcurrencyLimit(t *testing.T) {
	const concurrency = 3
	const totalJobs = 12

	jobs := make([]processing.Job, totalJobs)
	for i := 0; i < totalJobs; i++ {
		jobs[i] = processing.Job{
			ID:      uuid.New(),
			MediaID: uuid.New(),
			Type:    "transcode",
		}
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	var currentActive atomic.Int32
	var maxActive atomic.Int32
	var processedCount atomic.Int32

	registry.Register("transcode", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		active := currentActive.Add(1)
		for {
			max := maxActive.Load()
			if active <= max {
				break
			}
			if maxActive.CompareAndSwap(max, active) {
				break
			}
		}

		time.Sleep(30 * time.Millisecond)
		currentActive.Add(-1)
		processedCount.Add(1)
		return nil
	}))

	cfg := processing.Config{
		WorkerConcurrency: concurrency,
		QueueBuffer:       10,
		PollInterval:      10 * time.Millisecond,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	assert.Eventually(t, func() bool {
		return processedCount.Load() == int32(totalJobs)
	}, 1500*time.Millisecond, 20*time.Millisecond)

	engine.Stop()

	assert.Equal(t, int32(totalJobs), processedCount.Load())
	assert.LessOrEqual(t, maxActive.Load(), int32(concurrency), "Максимальное число одновременно выполняемых задач не должно превышать WORKER_CONCURRENCY")
}

// 2. Тест: Feeder claim-ит не больше свободной ёмкости (Backpressure)
func TestFeederBackpressure(t *testing.T) {
	const bufferSize = 4
	const totalJobs = 10

	jobs := make([]processing.Job, totalJobs)
	for i := 0; i < totalJobs; i++ {
		jobs[i] = processing.Job{
			ID:      uuid.New(),
			MediaID: uuid.New(),
			Type:    "thumbnail",
		}
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	var processedCount atomic.Int32

	registry.Register("thumbnail", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		time.Sleep(20 * time.Millisecond)
		processedCount.Add(1)
		return nil
	}))

	cfg := processing.Config{
		WorkerConcurrency: 1,
		QueueBuffer:       bufferSize,
		PollInterval:      5 * time.Millisecond,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	assert.Eventually(t, func() bool {
		return processedCount.Load() == int32(totalJobs)
	}, 1500*time.Millisecond, 20*time.Millisecond)

	engine.Stop()

	assert.LessOrEqual(t, repo.GetMaxClaimedLimit(), bufferSize, "Feeder не должен запрашивать у БД больше свободной ёмкости канала")
}

// 3. Тест: Handler registry различает типы jobs и отвергает неизвестный тип
func TestHandlerRegistryRejection(t *testing.T) {
	registry := processing.NewRegistry()

	knownHandled := false
	registry.Register("image_thumb", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		knownHandled = true
		return nil
	}))

	// Извлечение известного типа
	h, err := registry.Get("image_thumb")
	require.NoError(t, err)
	require.NotNil(t, h)
	require.NoError(t, h.Handle(context.Background(), processing.Job{}))
	assert.True(t, knownHandled)

	// Извлечение неизвестного типа -> ошибка ErrUnknownJobType
	hErr, err := registry.Get("unknown_type")
	require.Error(t, err)
	assert.Nil(t, hErr)
	assert.ErrorIs(t, err, processing.ErrUnknownJobType)
}

// 4. Тест: Метрики отражают channel depth, DB queue depth и in-flight workers
func TestMetricsReflection(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)

	jobs := []processing.Job{
		{ID: uuid.New(), MediaID: uuid.New(), Type: "slow_job"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	started := make(chan struct{})
	release := make(chan struct{})

	registry.Register("slow_job", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		close(started)
		<-release
		return nil
	}))

	cfg := processing.Config{
		WorkerConcurrency: 2,
		QueueBuffer:       8,
		PollInterval:      10 * time.Millisecond,
	}

	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	<-started

	// В процессе выполнения 1 задачи: in-flight workers = 1
	inFlightValue := getGaugeValue(t, metrics.InFlightWorkers)
	assert.Equal(t, float64(1), inFlightValue)

	close(release)
	engine.Stop()
}

func getGaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}
