package processing

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics содержит метрики Prometheus для движка обработки.
type Metrics struct {
	InFlightWorkers          prometheus.Gauge
	DBQueueDepth             prometheus.Gauge
	JobsProcessedTotal       prometheus.Counter
	JobsFailedTotal          prometheus.Counter
	LeaseExtensionsTotal     prometheus.Counter
	LeaseExtensionErrorsTotal prometheus.Counter
}

// NewMetrics создаёт и регистрирует метрики.
// Если reg == nil, используется глобальный реестр Prometheus.
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
