// регистрация метрик в Prometheus
package metrics

import "github.com/prometheus/client_golang/prometheus"

func init() {
	prometheus.MustRegister(
		RPCRequestsTotal,
		RPCDuration,
		ActiveStreams,
	)
}
