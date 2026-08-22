package processing

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics содержит метрики Prometheus для движка обработки.
type Metrics struct {
	ChannelDepth    prometheus.Gauge
	InFlightWorkers prometheus.Gauge
	DBQueueDepth    prometheus.Gauge
}

// NewMetrics создаёт и регистрирует метрики.
// Если reg == nil, используется глобальный реестр Prometheus.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	return &Metrics{
		ChannelDepth: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "media",
			Subsystem: "processing",
			Name:      "channel_depth",
			Help:      "Current number of jobs buffered in internal channel.",
		}),
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
	}
}
