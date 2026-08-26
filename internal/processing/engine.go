package processing

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

const finalizationTimeout = 5 * time.Second

// Config задаёт параметры работы ядра движка обработки.
type Config struct {
	WorkerConcurrency int
	PollInterval      time.Duration
	JobTimeout        time.Duration // таймаут на выполнение одной задачи
	LeaseDuration     time.Duration // длительность lease при claim; heartbeat тикает каждые LeaseDuration/3
}

// Engine представляет собой ядро движка обработки задач.
//
// Каждый воркер самостоятельно забирает задачу из БД (pull-on-demand),
// выполняет хендлер с отдельным контекстом и таймаутом, и финализирует
// результат (MarkDone/FailJob) на неотменяемом контексте.
type Engine struct {
	cfg      Config
	repo     JobRepository
	registry *Registry
	metrics  *Metrics

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// NewEngine создаёт и инициализирует новый Engine.
func NewEngine(cfg Config, repo JobRepository, registry *Registry, metrics *Metrics) *Engine {
	if cfg.WorkerConcurrency <= 0 {
		cfg.WorkerConcurrency = 2
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 10 * time.Minute
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}

	return &Engine{
		cfg:      cfg,
		repo:     repo,
		registry: registry,
		metrics:  metrics,
	}
}

// Start запускает воркеры под переданным контекстом.
func (e *Engine) Start(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	e.cancel = cancel

	for i := 0; i < e.cfg.WorkerConcurrency; i++ {
		e.wg.Add(1)
		go e.workerLoop(ctx, i)
	}

	slog.Info("processing engine started",
		"concurrency", e.cfg.WorkerConcurrency,
		"job_timeout", e.cfg.JobTimeout,
		"lease_duration", e.cfg.LeaseDuration,
		"heartbeat_interval", e.cfg.LeaseDuration/3,
	)

	return nil
}

// Stop останавливает работу воркеров.
// Воркеры перестают клеймить новые задачи, дорабатывают текущие
// (в пределах JobTimeout) и финализируют результаты.
func (e *Engine) Stop() {
	e.once.Do(func() {
		if e.cancel != nil {
			e.cancel()
		}
		e.wg.Wait()
		slog.Info("processing engine stopped")
	})
}

// workerLoop выполняет цикл работы отдельного воркера.
// Воркер сам клеймит задачу, когда свободен (pull-on-demand).
func (e *Engine) workerLoop(ctx context.Context, workerID int) {
	defer e.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := e.repo.ClaimOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("worker failed to claim job",
				"worker_id", workerID,
				"error", err,
			)
			e.sleep(ctx, e.cfg.PollInterval)
			continue
		}

		if job == nil {
			// Очередь пуста — обновляем метрику и ждём.
			if depth, err := e.repo.GetQueueDepth(ctx); err == nil {
				e.metrics.DBQueueDepth.Set(float64(depth))
			}
			e.sleep(ctx, e.cfg.PollInterval)
			continue
		}

		// Обновляем метрику глубины очереди в БД.
		if depth, err := e.repo.GetQueueDepth(ctx); err == nil {
			e.metrics.DBQueueDepth.Set(float64(depth))
		}

		e.processJob(job, workerID)
	}
}

// processJob выполняет хендлер с recover() и обновляет статус задачи.
// Хендлер получает отдельный контекст с таймаутом (не привязан к ctx движка).
// Heartbeat-горутина продлевает lease каждые LeaseDuration/3.
// Финализация (MarkDone/FailJob) выполняется на неотменяемом контексте.
func (e *Engine) processJob(job *Job, workerID int) {
	e.metrics.InFlightWorkers.Inc()
	defer e.metrics.InFlightWorkers.Dec()

	handler, err := e.registry.Get(job.Type)
	if err != nil {
		slog.Error("job rejected: unknown job type",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"error", err,
		)
		finCtx, finCancel := context.WithTimeout(context.Background(), finalizationTimeout)
		defer finCancel()
		if failErr := e.repo.FailJob(finCtx, job.ID.String(), "unknown job type: "+job.Type); failErr != nil {
			slog.Error("failed to mark job as failed", "job_id", job.ID.String(), "error", failErr)
		}
		e.metrics.JobsFailedTotal.Inc()
		return
	}

	// Отдельный контекст для задачи — не привязан к ctx движка.
	jobCtx, jobCancel := context.WithTimeout(context.Background(), e.cfg.JobTimeout)

	// Heartbeat: продлеваем lease в фоне, пока handler работает.
	heartbeatDone := make(chan struct{})
	go e.heartbeatLoop(jobCtx, job.ID.String(), workerID, heartbeatDone)

	// Запускаем хендлер с recover().
	handlerErr := e.safeHandle(jobCtx, handler, *job, workerID)
	jobCancel()

	// Останавливаем heartbeat и ждём его завершения.
	<-heartbeatDone

	// Финализация на неотменяемом контексте — гарантирует запись в БД.
	finCtx, finCancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer finCancel()

	if handlerErr != nil {
		slog.Error("job handling error",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"job_type", job.Type,
			"error", handlerErr,
		)
		if failErr := e.repo.FailJob(finCtx, job.ID.String(), handlerErr.Error()); failErr != nil {
			slog.Error("failed to mark job as failed", "job_id", job.ID.String(), "error", failErr)
		}
		e.metrics.JobsFailedTotal.Inc()
		return
	}

	if err := e.repo.MarkDone(finCtx, job.ID.String()); err != nil {
		slog.Error("failed to mark job as done",
			"worker_id", workerID,
			"job_id", job.ID.String(),
			"error", err,
		)
	}
	e.metrics.JobsProcessedTotal.Inc()
}

// heartbeatLoop продлевает lease задачи каждые LeaseDuration/3.
// Останавливается при отмене ctx (handler завершился или таймаут).
// Закрывает done канал при выходе.
func (e *Engine) heartbeatLoop(ctx context.Context, jobID string, workerID int, done chan<- struct{}) {
	defer close(done)

	interval := e.cfg.LeaseDuration / 3
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Используем фоновый контекст, чтобы heartbeat мог завершиться
			// даже если jobCtx уже отменён (race между ticker и cancel).
			extendCtx, extendCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := e.repo.ExtendLease(extendCtx, jobID, e.cfg.LeaseDuration)
			extendCancel()

			if err != nil {
				e.metrics.LeaseExtensionErrorsTotal.Inc()
				slog.Warn("heartbeat: failed to extend lease",
					"worker_id", workerID,
					"job_id", jobID,
					"error", err,
				)
				return
			}
			e.metrics.LeaseExtensionsTotal.Inc()
		}
	}
}

// safeHandle оборачивает вызов handler.Handle в recover().
// При панике возвращает ошибку, воркер продолжает работу.
func (e *Engine) safeHandle(ctx context.Context, handler Handler, job Job, workerID int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			slog.Error("panic in job handler",
				"worker_id", workerID,
				"job_id", job.ID.String(),
				"job_type", job.Type,
				"panic", r,
				"stack", string(stack),
			)
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return handler.Handle(ctx, job)
}

// sleep ждёт указанную длительность или завершения контекста.
func (e *Engine) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
