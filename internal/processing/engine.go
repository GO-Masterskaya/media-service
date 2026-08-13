package processing

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Config задаёт параметры работы ядра движка обработки.
type Config struct {
	WorkerConcurrency int
	QueueBuffer       int
	PollInterval      time.Duration
}

// Engine представляет собой ядро движка обработки задач.
type Engine struct {
	cfg      Config
	repo     JobRepository
	registry *Registry
	metrics  *Metrics
	jobCh    chan Job

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// NewEngine создаёт и инициализирует новый Engine.
func NewEngine(cfg Config, repo JobRepository, registry *Registry, metrics *Metrics) *Engine {
	if cfg.WorkerConcurrency <= 0 {
		cfg.WorkerConcurrency = 2
	}
	if cfg.QueueBuffer <= 0 {
		cfg.QueueBuffer = 64
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &Engine{
		cfg:      cfg,
		repo:     repo,
		registry: registry,
		metrics:  metrics,
		jobCh:    make(chan Job, cfg.QueueBuffer),
	}
}

// Start запускает воркеры и податчик задач (Feeder) под переданным контекстом.
func (e *Engine) Start(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	e.cancel = cancel

	// 1. Запуск пула воркеров (не больше WORKER_CONCURRENCY)
	for i := 0; i < e.cfg.WorkerConcurrency; i++ {
		e.wg.Add(1)
		go e.workerLoop(ctx, i)
	}

	// 2. Запуск фонового Feeder
	e.wg.Add(1)
	go e.feederLoop(ctx)

	slog.Info("processing engine started",
		"concurrency", e.cfg.WorkerConcurrency,
		"buffer", e.cfg.QueueBuffer,
	)

	return nil
}

// Stop останавливает работу Feeder и воркеров.
func (e *Engine) Stop() {
	e.once.Do(func() {
		if e.cancel != nil {
			e.cancel()
		}
		e.wg.Wait()
		slog.Info("processing engine stopped")
	})
}

// feederLoop периодически опрашивает БД и дозагружает задачи в jobCh в пределах свободной ёмкости.
func (e *Engine) feederLoop(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		e.fetchAndDistribute(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// fetchAndDistribute запрашивает свободные слоты и подтягивает задачи из БД.
func (e *Engine) fetchAndDistribute(ctx context.Context) {
	// Метрика глубины локального канала
	e.metrics.ChannelDepth.Set(float64(len(e.jobCh)))

	// Метрика глубины очереди в БД
	if depth, err := e.repo.GetQueueDepth(ctx); err == nil {
		e.metrics.DBQueueDepth.Set(float64(depth))
	}

	// Backpressure: вычисляем только СВОБОДНУЮ ёмкость канала
	free := cap(e.jobCh) - len(e.jobCh)
	if free <= 0 {
		return
	}

	jobs, err := e.repo.ClaimQueued(ctx, free)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("feeder failed to claim queued jobs", "error", err)
		}
		return
	}

	for _, job := range jobs {
		select {
		case e.jobCh <- job:
			e.metrics.ChannelDepth.Set(float64(len(e.jobCh)))
		case <-ctx.Done():
			return
		}
	}
}

// workerLoop выполняет цикл работы отдельного воркера из пула.
//
// Примечание: при отмене ctx select может выбрать ветку jobCh, если оба канала
// готовы одновременно. Это допустимо — graceful drain с доработкой задач
// реализуется в задаче #26.
func (e *Engine) workerLoop(ctx context.Context, workerID int) {
	defer e.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-e.jobCh:
			if !ok {
				return
			}
			e.processJob(ctx, job, workerID)
		}
	}
}

// processJob выполняет вызов зарегистрированного хэндлера и обновляет метрики in-flight.
func (e *Engine) processJob(ctx context.Context, job Job, workerID int) {
	e.metrics.InFlightWorkers.Inc()
	defer e.metrics.InFlightWorkers.Dec()
	defer e.metrics.ChannelDepth.Set(float64(len(e.jobCh)))

	handler, err := e.registry.Get(job.Type)
	if err != nil {
		slog.Error("job rejected: unknown job type",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"error", err,
		)
		return
	}

	if err := handler.Handle(ctx, job); err != nil {
		slog.Error("job handling error",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"error", err,
		)
		return
	}
}
