package processing_test

import (
	"context"
	"errors"
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
// Реализует pull-on-demand интерфейс (ClaimOne вместо ClaimQueued).
type MockJobRepository struct {
	mu              sync.Mutex
	queuedJobs      []processing.Job
	doneJobs        map[string]bool   // jobID -> true
	failedJobs      map[string]string // jobID -> reason
	released        map[string]bool   // jobID -> true
	leaseExtensions int               // количество вызовов ExtendLease
}

func NewMockJobRepository(jobs []processing.Job) *MockJobRepository {
	return &MockJobRepository{
		queuedJobs: jobs,
		doneJobs:   make(map[string]bool),
		failedJobs: make(map[string]string),
		released:   make(map[string]bool),
	}
}

func (m *MockJobRepository) ClaimOne(_ context.Context) (*processing.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queuedJobs) == 0 {
		return nil, nil
	}

	job := m.queuedJobs[0]
	m.queuedJobs = m.queuedJobs[1:]
	return &job, nil
}

func (m *MockJobRepository) GetQueueDepth(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.queuedJobs)), nil
}

func (m *MockJobRepository) MarkDone(_ context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doneJobs[jobID] = true
	return nil
}

func (m *MockJobRepository) FailJob(_ context.Context, jobID string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedJobs[jobID] = reason
	return nil
}

func (m *MockJobRepository) ReleaseJob(_ context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released[jobID] = true
	return nil
}

func (m *MockJobRepository) ExtendLease(_ context.Context, jobID string, d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaseExtensions++
	return nil
}

func (m *MockJobRepository) GetLeaseExtensions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leaseExtensions
}

func (m *MockJobRepository) GetDoneJobs() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]bool, len(m.doneJobs))
	for k, v := range m.doneJobs {
		cp[k] = v
	}
	return cp
}

func (m *MockJobRepository) GetFailedJobs() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]string, len(m.failedJobs))
	for k, v := range m.failedJobs {
		cp[k] = v
	}
	return cp
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
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	assert.Eventually(t, func() bool {
		return processedCount.Load() == int32(totalJobs)
	}, 4*time.Second, 20*time.Millisecond)

	engine.Stop()

	assert.Equal(t, int32(totalJobs), processedCount.Load())
	assert.LessOrEqual(t, maxActive.Load(), int32(concurrency), "Максимальное число одновременно выполняемых задач не должно превышать WORKER_CONCURRENCY")
}

// 2. Тест: Воркер клеймит ровно по одной задаче (pull-on-demand)
func TestPullOnDemandClaimOne(t *testing.T) {
	const totalJobs = 6

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
		time.Sleep(10 * time.Millisecond)
		processedCount.Add(1)
		return nil
	}))

	cfg := processing.Config{
		WorkerConcurrency: 2,
		PollInterval:      5 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	assert.Eventually(t, func() bool {
		return processedCount.Load() == int32(totalJobs)
	}, 4*time.Second, 20*time.Millisecond)

	engine.Stop()

	assert.Equal(t, int32(totalJobs), processedCount.Load())
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

// 4. Тест: Метрики отражают in-flight workers
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
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func getCounterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	return m.GetCounter().GetValue()
}

// 5. Тест: неизвестный тип задачи помечается как failed
func TestUnknownJobTypeMarkedFailed(t *testing.T) {
	jobID := uuid.New()
	jobs := []processing.Job{
		{ID: jobID, MediaID: uuid.New(), Type: "completely_unknown_type"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()
	// Не регистрируем handler для "completely_unknown_type"

	cfg := processing.Config{
		WorkerConcurrency: 1,
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	// Ждём, пока job будет обработан
	assert.Eventually(t, func() bool {
		failed := repo.GetFailedJobs()
		_, ok := failed[jobID.String()]
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	engine.Stop()

	failed := repo.GetFailedJobs()
	reason, ok := failed[jobID.String()]
	assert.True(t, ok, "Задача с неизвестным типом должна быть помечена как failed")
	assert.Contains(t, reason, "unknown job type")
}

// 6. Тест: ошибка handler помечает задачу как failed
func TestHandlerErrorMarkedFailed(t *testing.T) {
	jobID := uuid.New()
	jobs := []processing.Job{
		{ID: jobID, MediaID: uuid.New(), Type: "failing_handler"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	handlerErr := errors.New("ffmpeg exit code 1")
	registry.Register("failing_handler", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		return handlerErr
	}))

	cfg := processing.Config{
		WorkerConcurrency: 1,
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	assert.Eventually(t, func() bool {
		failed := repo.GetFailedJobs()
		_, ok := failed[jobID.String()]
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	engine.Stop()

	failed := repo.GetFailedJobs()
	reason, ok := failed[jobID.String()]
	assert.True(t, ok, "Задача с ошибкой handler должна быть помечена как failed")
	assert.Contains(t, reason, "ffmpeg exit code 1")
}

// 7. Тест: успешная задача помечается как done (MarkDone)
func TestSuccessfulJobMarkedDone(t *testing.T) {
	jobID := uuid.New()
	jobs := []processing.Job{
		{ID: jobID, MediaID: uuid.New(), Type: "transcode"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	registry.Register("transcode", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		return nil // успех
	}))

	cfg := processing.Config{
		WorkerConcurrency: 1,
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	assert.Eventually(t, func() bool {
		done := repo.GetDoneJobs()
		return done[jobID.String()]
	}, 2*time.Second, 20*time.Millisecond)

	engine.Stop()

	done := repo.GetDoneJobs()
	assert.True(t, done[jobID.String()], "Успешная задача должна быть помечена как done")

	// Проверяем метрику processed
	assert.Equal(t, float64(1), getCounterValue(t, metrics.JobsProcessedTotal))
}

// 8. Тест: паника в handler не роняет воркер, задача помечается как failed
func TestPanicInHandlerRecovery(t *testing.T) {
	panicJobID := uuid.New()
	normalJobID := uuid.New()

	jobs := []processing.Job{
		{ID: panicJobID, MediaID: uuid.New(), Type: "panicker"},
		{ID: normalJobID, MediaID: uuid.New(), Type: "normal"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	registry.Register("panicker", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		panic("unexpected nil pointer in ffmpeg wrapper")
	}))

	registry.Register("normal", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		return nil
	}))

	cfg := processing.Config{
		WorkerConcurrency: 1,
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	// Ждём, пока обе задачи будут обработаны
	assert.Eventually(t, func() bool {
		failed := repo.GetFailedJobs()
		done := repo.GetDoneJobs()
		return failed[panicJobID.String()] != "" && done[normalJobID.String()]
	}, 3*time.Second, 20*time.Millisecond)

	engine.Stop()

	// Паникующая задача — failed
	failed := repo.GetFailedJobs()
	reason, ok := failed[panicJobID.String()]
	assert.True(t, ok, "Паникующая задача должна быть помечена как failed")
	assert.Contains(t, reason, "panic:")

	// Нормальная задача — done (воркер выжил после паники)
	done := repo.GetDoneJobs()
	assert.True(t, done[normalJobID.String()], "Нормальная задача после паники должна быть обработана")
}

// 9. Тест: таймаут на задачу — зависший handler отменяется по JobTimeout
func TestJobTimeout(t *testing.T) {
	jobID := uuid.New()
	jobs := []processing.Job{
		{ID: jobID, MediaID: uuid.New(), Type: "slow"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	registry.Register("slow", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		// Имитируем зависший ffmpeg — ждём отмены контекста.
		<-ctx.Done()
		return ctx.Err()
	}))

	cfg := processing.Config{
		WorkerConcurrency: 1,
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        200 * time.Millisecond, // короткий таймаут для теста
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	// Ждём, пока задача будет помечена как failed по таймауту
	assert.Eventually(t, func() bool {
		failed := repo.GetFailedJobs()
		_, ok := failed[jobID.String()]
		return ok
	}, 3*time.Second, 20*time.Millisecond)

	engine.Stop()

	failed := repo.GetFailedJobs()
	reason, ok := failed[jobID.String()]
	assert.True(t, ok, "Задача с таймаутом должна быть помечена как failed")
	assert.Contains(t, reason, "context deadline exceeded")
}

// 10. Тест: heartbeat продлевает lease для длительных задач
func TestHeartbeatExtendsLease(t *testing.T) {
	jobID := uuid.New()
	jobs := []processing.Job{
		{ID: jobID, MediaID: uuid.New(), Type: "long_video"},
	}

	repo := NewMockJobRepository(jobs)
	registry := processing.NewRegistry()

	registry.Register("long_video", processing.HandlerFunc(func(ctx context.Context, job processing.Job) error {
		// Задача длится 500ms — это больше чем lease (150ms).
		// Heartbeat должен сработать ≥1 раз (интервал 50ms = 150ms/3).
		time.Sleep(500 * time.Millisecond)
		return nil
	}))

	cfg := processing.Config{
		WorkerConcurrency: 1,
		PollInterval:      10 * time.Millisecond,
		JobTimeout:        5 * time.Second,
		LeaseDuration:     150 * time.Millisecond, // короткий lease для теста
	}

	reg := prometheus.NewRegistry()
	metrics := processing.NewMetrics(reg)
	engine := processing.NewEngine(cfg, repo, registry, metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, engine.Start(ctx))

	// Ждём, пока задача будет обработана.
	assert.Eventually(t, func() bool {
		done := repo.GetDoneJobs()
		return done[jobID.String()]
	}, 3*time.Second, 20*time.Millisecond)

	engine.Stop()

	// Проверяем, что heartbeat вызывался.
	extensions := repo.GetLeaseExtensions()
	assert.GreaterOrEqual(t, extensions, 1, "Heartbeat должен был продлить lease хотя бы 1 раз")

	// Проверяем метрику.
	assert.GreaterOrEqual(t, getCounterValue(t, metrics.LeaseExtensionsTotal), float64(1),
		"Метрика lease_extensions_total должна быть ≥1")

	// Задача должна быть done, а не failed.
	done := repo.GetDoneJobs()
	assert.True(t, done[jobID.String()], "Длительная задача с heartbeat должна быть done, а не зациклена")
}
