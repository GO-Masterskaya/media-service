package media

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"

	"sync/atomic"
)

var (
	reconcilerMetricsOnce sync.Once
	recScanned            *prometheus.CounterVec
	recDeleted            *prometheus.CounterVec
	recFailed             *prometheus.CounterVec
	recOrphans            prometheus.Counter
)

func initReconcilerMetrics() {
	reconcilerMetricsOnce.Do(func() {
		recScanned = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconciler_scanned_total",
			Help: "Total records/objects scanned by reconciler",
		}, []string{"kind"})
		recDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconciler_deleted_total",
			Help: "Total deleted by reconciler",
		}, []string{"kind"})
		recFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconciler_failed_total",
			Help: "Total failures",
		}, []string{"kind"})
		recOrphans = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reconciler_orphans_found_total",
			Help: "Total orphan objects found",
		})
		prometheus.MustRegister(recScanned, recDeleted, recFailed, recOrphans)
	})
}

type ReconcilerConfig struct {
	Interval    time.Duration
	GracePeriod time.Duration
	BatchSize   int
	DryRun      bool
}

type Reconciler struct {
	mediaRepo repo.MediaRepo
	storage   storage.Interface
	cfg       ReconcilerConfig
	log       *slog.Logger

	scannedCounter *prometheus.CounterVec
	deletedCounter *prometheus.CounterVec
	failedCounter  *prometheus.CounterVec
	orphanCounter  prometheus.Counter

	tickRunning atomic.Bool
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func NewReconciler(mediaRepo repo.MediaRepo, storage storage.Interface, cfg ReconcilerConfig, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = 1 * time.Hour
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 1 * time.Hour
	}
	initReconcilerMetrics()

	return &Reconciler{
		mediaRepo:      mediaRepo,
		storage:        storage,
		cfg:            cfg,
		log:            log,
		scannedCounter: recScanned,
		deletedCounter: recDeleted,
		failedCounter:  recFailed,
		orphanCounter:  recOrphans,
		stopCh:         make(chan struct{}),
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.reconcile(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reconciler stopped (context done)")
			return
		case <-r.stopCh:
			r.log.Info("reconciler stopped (shutdown requested)")
			return
		case <-ticker.C:
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				r.reconcile(ctx)
			}()
		}
	}
}

// Shutdown останавливает тикер и дожидается завершения текущего tick.
// Идемпотентна: повторный вызов не паникует.
func (r *Reconciler) Shutdown(ctx context.Context) error {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		r.log.Info("reconciler shutdown gracefully")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("reconciler shutdown timeout: %w", ctx.Err())
	}
}

func (r *Reconciler) reconcile(ctx context.Context) {
	if !r.tickRunning.CompareAndSwap(false, true) {
		r.log.Warn("reconciler tick skipped: previous tick still running")
		return
	}
	defer r.tickRunning.Store(false)

	r.log.Info("reconciler tick started", slog.Bool("dry_run", r.cfg.DryRun))
	r.reconcileDeleting(ctx)
	r.reconcileOrphans(ctx)
	r.log.Info("reconciler tick finished")
}

// ---------- deleting records ----------

func (r *Reconciler) reconcileDeleting(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-r.cfg.GracePeriod)
	mediaList, err := r.mediaRepo.ListDeleting(ctx, cutoff, r.cfg.BatchSize)
	if err != nil {
		r.log.Error("list deleting failed", slog.Any("error", err))
		r.failedCounter.WithLabelValues("db").Inc()
		return
	}

	r.scannedCounter.WithLabelValues("db").Add(float64(len(mediaList)))

	for _, m := range mediaList {
		select {
		case <-ctx.Done():
			r.log.Info("reconcile deleting interrupted by shutdown")
			return
		default:
		}
		if r.cfg.DryRun {
			r.log.Info("dry-run: would delete media",
				slog.String("media_id", m.ID.String()),
				slog.String("prefix", path.Join(m.OwnerID.String(), m.ID.String())+"/"),
			)
			continue
		}
		if err := r.processDeletingMedia(ctx, m); err != nil {
			r.log.Error("process deleting media failed",
				slog.Any("error", err),
				slog.String("media_id", m.ID.String()),
			)
			r.failedCounter.WithLabelValues("db").Inc()
			continue
		}
		r.deletedCounter.WithLabelValues("db").Inc()
	}
}

func (r *Reconciler) processDeletingMedia(ctx context.Context, m *repo.Media) error {
	// Guard: storage_key должен принадлежать owner/media.
	expectedPrefix := path.Join(m.OwnerID.String(), m.ID.String()) + "/"
	if !strings.HasPrefix(m.StorageKey, expectedPrefix) {
		return fmt.Errorf("ownership guard: storage key %q does not match owner %s / media %s", m.StorageKey, m.OwnerID, m.ID)
	}
	if !r.isServiceKey(m.StorageKey) {
		return fmt.Errorf("ownership guard: key %q outside service prefix", m.StorageKey)
	}

	prefix := path.Join(m.OwnerID.String(), m.ID.String()) + "/"
	if err := r.storage.DeletePrefix(ctx, prefix); err != nil {
		return fmt.Errorf("delete prefix: %w", err)
	}

	if err := r.mediaRepo.HardDelete(ctx, m.ID); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("hard delete media: %w", err)
	}

	r.log.Info("deleted media and objects",
		slog.String("media_id", m.ID.String()),
		slog.String("owner_id", m.OwnerID.String()),
	)
	return nil
}

// ---------- orphan objects ----------

type orphanCandidate struct {
	key          string
	mediaID      uuid.UUID
	lastModified time.Time
}

// reconcileOrphans удаляет объекты в MinIO, для которых нет записей в БД.
//
// ВАЖНО: этот метод полагается на контракт ingest (#7):
//
//	«строка в media создаётся ДО или ВМЕСТЕ с объектом в MinIO».
//
// Если ingest нарушает порядок (объект до строки), grace period (default 1h)
// защищает только от race, но не от систематической потери данных.
// При изменении ingest-логики — пересмотреть grace period или добавить
// статус 'storing', который reconciler игнорирует.
func (r *Reconciler) reconcileOrphans(ctx context.Context) {
	var batch []orphanCandidate

	err := r.storage.ForEachObject(ctx, "", func(obj storage.ObjectInfo) error {
		// Проверка отмены контекста между объектами.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !r.isServiceKey(obj.Key) {
			return nil
		}
		mediaID, ok := r.extractMediaID(obj.Key)
		if !ok {
			return nil
		}
		// Если UploadStartedAt не заполнен — невозможно определить grace period,
		// пропускаем, чтобы не удалить in-flight объект.
		if obj.UploadStartedAt.IsZero() {
			r.log.Warn("orphan check: UploadStartedAt is zero, skipping",
				slog.String("key", obj.Key),
			)
			return nil
		}
		// grace от старта загрузки, не от LastModified
		if time.Since(obj.UploadStartedAt) < r.cfg.GracePeriod {
			return nil // защита in-flight
		}

		batch = append(batch, orphanCandidate{key: obj.Key, mediaID: mediaID, lastModified: obj.LastModified})
		if len(batch) >= r.cfg.BatchSize {
			if err := r.processOrphanBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		r.log.Error("orphan scan failed", slog.Any("error", err))
		r.failedCounter.WithLabelValues("storage").Inc()
		return
	}
	if len(batch) > 0 {
		if berr := r.processOrphanBatch(ctx, batch); berr != nil {
			r.log.Error("orphan batch failed", slog.Any("error", berr))
			r.failedCounter.WithLabelValues("storage").Inc()
		}
	}
}

func (r *Reconciler) processOrphanBatch(ctx context.Context, batch []orphanCandidate) error {
	ids := make([]uuid.UUID, 0, len(batch))
	for _, c := range batch {
		ids = append(ids, c.mediaID)
	}

	exists, err := r.mediaRepo.ExistsBatch(ctx, ids)
	if err != nil {
		r.log.Error("exists batch failed", slog.Any("error", err))
		r.failedCounter.WithLabelValues("storage").Add(float64(len(batch)))
		return err
	}

	for _, c := range batch {
		select {
		case <-ctx.Done():
			r.log.Info("orphan batch interrupted by shutdown")
			return nil
		default:
		}
		if _, ok := exists[c.mediaID]; ok {
			continue
		}

		r.orphanCounter.Inc()

		if r.cfg.DryRun {
			r.log.Info("dry-run: would delete orphan",
				slog.String("key", c.key),
				slog.Time("last_modified", c.lastModified),
			)
			continue
		}

		if err := r.storage.DeleteObject(ctx, c.key); err != nil {
			r.log.Error("delete orphan failed", slog.Any("error", err), slog.String("key", c.key))
			r.failedCounter.WithLabelValues("storage").Inc()
			continue
		}
		r.deletedCounter.WithLabelValues("orphan").Inc()
		r.log.Info("deleted orphan object",
			slog.String("key", c.key),
			slog.Time("last_modified", c.lastModified),
		)
	}

	return nil
}

// ---------- guards ----------

func (r *Reconciler) isServiceKey(key string) bool {
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return false
	}
	_, err1 := uuid.Parse(parts[0])
	_, err2 := uuid.Parse(parts[1])
	return err1 == nil && err2 == nil
}

func (r *Reconciler) extractMediaID(key string) (uuid.UUID, bool) {
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(parts[1])
	return id, err == nil
}
