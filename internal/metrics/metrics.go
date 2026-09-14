package metrics

import "github.com/prometheus/client_golang/prometheus"

type GRPCMetrics struct {
	RPCRequestsTotal *prometheus.CounterVec
	RPCDuration      *prometheus.HistogramVec
	ActiveStreams    prometheus.Gauge
}

func NewGRPCMetrics(registerer prometheus.Registerer) *GRPCMetrics {
	m := &GRPCMetrics{
		RPCRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpc_requests_total",
				Help: "Total number of gRPC requests.",
			},
			[]string{"method", "code"},
		),

		RPCDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "grpc_request_duration_seconds",
				Help: "Duration of gRPC requests.",
				Buckets: []float64{
					0.01,
					0.025,
					0.05,
					0.1,
					0.25,
					0.5,
					1,
					2.5,
					5,
					10,
					30,
					60,
					120,
					300,
					600,
					1800,
				},
			},
			[]string{"method"},
		),

		ActiveStreams: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "grpc_active_streams",
				Help: "Number of active gRPC streams.",
			},
		),
	}

	registerer.MustRegister(
		m.RPCRequestsTotal,
		m.RPCDuration,
		m.ActiveStreams,
	)

	return m
}
