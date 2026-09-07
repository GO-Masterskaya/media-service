// определяем, какие метрики у нас есть
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var RPCRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "grpc_requests_total",
		Help: "Total number of gRPC requests.",
	},
	[]string{"method", "code"},
)

var RPCDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "grpc_request_duration_seconds",
		Help: "Duration of gRPC requests.",
	},
	[]string{"method"},
)

var ActiveStreams = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "grpc_active_streams",
		Help: "Number of active gRPC streams.",
	},
)
