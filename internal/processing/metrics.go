package processing

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics содержит метрики Prometheus для движка обработки.
type Metrics struct {
	InFlightWorkers           prometheus.Gauge
	DBQueueDepth              prometheus.Gauge
	JobsProcessedTotal        prometheus.Counter
	JobsFailedTotal           prometheus.Counter
	JobsRetriedTotal          prometheus.Counter
	JobsRecoveredTotal        prometheus.Counter
	ShutdownJobsReleasedTotal prometheus.Counter
	ShutdownGracefulTotal     prometheus.Counter
	ShutdownTimeoutTotal      prometheus.Counter
	ProcessingDuration        prometheus.Histogram
	LeaseExtensionsTotal      prometheus.Counter
	LeaseExtensionErrorsTotal prometheus.Counter
}

// NewMetrics создаёт и регистрирует метрики.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	return &Metrics{
		InFlightWorkers: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "in_flight_workers",
			Help:      "Current number of workers actively executing jobs.",
		}),
		DBQueueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "db_queue_depth",
			Help:      "Current number of queued jobs in database.",
		}),
		JobsProcessedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "jobs_processed_total",
			Help:      "Total number of successfully processed jobs.",
		}),
		JobsFailedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "jobs_failed_total",
			Help:      "Total number of failed jobs.",
		}),
		JobsRetriedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "jobs_retried_total",
			Help:      "Total number of jobs released back to queue for retry.",
		}),
		JobsRecoveredTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "jobs_recovered_total",
			Help:      "Total number of stale running jobs recovered to queued.",
		}),
		ShutdownJobsReleasedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "shutdown_jobs_released_total",
			Help:      "Total number of in-flight jobs released on shutdown.",
		}),
		ShutdownGracefulTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "shutdown_graceful_total",
			Help:      "Total number of graceful engine shutdowns within timeout.",
		}),
		ShutdownTimeoutTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "shutdown_timeout_total",
			Help:      "Total number of engine shutdowns that hit the deadline.",
		}),
		ProcessingDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "processing_duration_seconds",
			Help:      "Duration of job handler execution in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12),
		}),
		LeaseExtensionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "lease_extensions_total",
			Help:      "Total number of successful lease extensions (heartbeats).",
		}),
		LeaseExtensionErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "lease_extension_errors_total",
			Help:      "Total number of failed lease extension attempts.",
		}),
	}
}
