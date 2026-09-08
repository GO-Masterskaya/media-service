package upload

// metrics.go содержит Prometheus метрики для мониторинга upload temp storage.

import "github.com/prometheus/client_golang/prometheus"

// Metrics содержит Prometheus метрики для upload temp storage.
type Metrics struct {
	// TempFilesActive — количество активных temp файлов прямо сейчас.
	TempFilesActive prometheus.Gauge

	// TempBytesActive — суммарный размер активных temp файлов на диске.
	TempBytesActive prometheus.Gauge

	// CleanupTotal — количество операций cleanup (startup + periodic).
	// Labels: result="ok"|"error"
	CleanupTotal *prometheus.CounterVec

	// CleanupFilesTotal — количество удалённых stale файлов при cleanup.
	CleanupFilesTotal prometheus.Counter

	// DiskFullTotal — сколько раз получили ENOSPC при записи.
	DiskFullTotal prometheus.Counter

	// SizeLimitExceededTotal — сколько раз upload превысил size limit.
	SizeLimitExceededTotal prometheus.Counter
}

// NewMetrics создаёт и регистрирует все метрики в указанном registerer.
// Если registerer nil — используется prometheus.DefaultRegisterer.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	m := &Metrics{
		TempFilesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "media",
			Subsystem: "upload",
			Name:      "temp_files_active",
			Help:      "Number of currently active temp files on disk.",
		}),
		TempBytesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "media",
			Subsystem: "upload",
			Name:      "temp_bytes_active",
			Help:      "Total bytes of currently active temp files on disk.",
		}),
		CleanupTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "upload",
			Name:      "cleanup_total",
			Help:      "Number of cleanup operations performed.",
		}, []string{"result"}),
		CleanupFilesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "upload",
			Name:      "cleanup_files_total",
			Help:      "Total number of stale files removed by cleanup.",
		}),
		DiskFullTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "upload",
			Name:      "disk_full_total",
			Help:      "Number of times ENOSPC was encountered during upload.",
		}),
		SizeLimitExceededTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "media",
			Subsystem: "upload",
			Name:      "size_limit_exceeded_total",
			Help:      "Number of times upload exceeded the declared size limit.",
		}),
	}

	reg.MustRegister(
		m.TempFilesActive,
		m.TempBytesActive,
		m.CleanupTotal,
		m.CleanupFilesTotal,
		m.DiskFullTotal,
		m.SizeLimitExceededTotal,
	)

	return m
}
