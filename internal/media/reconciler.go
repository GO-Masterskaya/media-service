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
}

func NewReconciler(mediaRepo repo.MediaRepo, storage storage.Interface, cfg ReconcilerConfig, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
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
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	r.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reconciler stopped")
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) {
	r.log.Info("reconciler tick started")
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

func (r *Reconciler) reconcileOrphans(ctx context.Context) {
	objects, err := r.storage.ListObjects(ctx, "")
	if err != nil {
		r.log.Error("list objects failed", slog.Any("error", err))
		r.failedCounter.WithLabelValues("storage").Inc()
		return
	}

	var batch []orphanCandidate
	flush := func() {
		if len(batch) > 0 {
			r.processOrphanBatch(ctx, batch)
			batch = batch[:0]
		}
	}

	for _, obj := range objects {
		if !r.isServiceKey(obj.Key) {
			continue
		}
		mediaID, ok := r.extractMediaID(obj.Key)
		if !ok {
			continue
		}
		if time.Since(obj.LastModified) < r.cfg.GracePeriod {
			continue
		}
		batch = append(batch, orphanCandidate{key: obj.Key, mediaID: mediaID, lastModified: obj.LastModified})
		if len(batch) >= r.cfg.BatchSize {
			flush()
		}
	}
	flush()
}

func (r *Reconciler) processOrphanBatch(ctx context.Context, batch []orphanCandidate) {
	ids := make([]uuid.UUID, 0, len(batch))
	for _, c := range batch {
		ids = append(ids, c.mediaID)
	}

	exists, err := r.mediaRepo.ExistsBatch(ctx, ids)
	if err != nil {
		r.log.Error("exists batch failed", slog.Any("error", err))
		r.failedCounter.WithLabelValues("storage").Add(float64(len(batch)))
		return
	}

	for _, c := range batch {
		if _, ok := exists[c.mediaID]; ok {
			continue
		}

		r.orphanCounter.Inc()
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
