package metrics

import (
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestRPCRequestsTotal(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewGRPCMetrics(registry)

	m.RPCRequestsTotal.
		WithLabelValues("/media.v1.MediaService/GetMedia", "OK").
		Inc()

	m.RPCRequestsTotal.
		WithLabelValues("/media.v1.MediaService/GetMedia", "OK").
		Inc()

	m.RPCRequestsTotal.
		WithLabelValues("/media.v1.MediaService/GetMedia", "NotFound").
		Inc()

	got := testutil.ToFloat64(
		m.RPCRequestsTotal.WithLabelValues(
			"/media.v1.MediaService/GetMedia",
			"OK",
		),
	)

	if got != 2 {
		t.Errorf("expected 2 requests, got %v", got)
	}
}

func TestRPCDuration(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewGRPCMetrics(registry)

	method := "/media.v1.MediaService/GetMedia"

	m.RPCDuration.WithLabelValues(method).Observe(0.1)
	m.RPCDuration.WithLabelValues(method).Observe(0.2)

	ch := make(chan prometheus.Metric, 1)
	m.RPCDuration.Collect(ch)

	metric := <-ch

	var dtoMetric dto.Metric
	if err := metric.Write(&dtoMetric); err != nil {
		t.Fatalf("failed to read histogram: %v", err)
	}

	histogram := dtoMetric.GetHistogram()

	if histogram.GetSampleCount() != 2 {
		t.Errorf(
			"expected 2 observations, got %d",
			histogram.GetSampleCount(),
		)
	}

	if math.Abs(histogram.GetSampleSum()-0.3) > 1e-9 {
		t.Errorf(
			"expected duration sum approximately 0.3, got %v",
			histogram.GetSampleSum(),
		)
	}
}

func TestActiveStreams(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewGRPCMetrics(registry)

	m.ActiveStreams.Inc()
	m.ActiveStreams.Inc()

	if got := testutil.ToFloat64(m.ActiveStreams); got != 2 {
		t.Errorf("expected 2 active streams, got %v", got)
	}

	m.ActiveStreams.Dec()

	if got := testutil.ToFloat64(m.ActiveStreams); got != 1 {
		t.Errorf("expected 1 active stream, got %v", got)
	}

	m.ActiveStreams.Dec()

	if got := testutil.ToFloat64(m.ActiveStreams); got != 0 {
		t.Errorf("expected 0 active streams, got %v", got)
	}
}

func TestNewGRPCMetricsRegistersCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewGRPCMetrics(registry)

	m.RPCRequestsTotal.
		WithLabelValues("/media.v1.MediaService/GetMedia", "OK").
		Inc()

	m.RPCDuration.
		WithLabelValues("/media.v1.MediaService/GetMedia").
		Observe(0.1)

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	got := make(map[string]bool)

	for _, family := range metricFamilies {
		got[family.GetName()] = true
	}

	for _, name := range []string{
		"grpc_requests_total",
		"grpc_request_duration_seconds",
		"grpc_active_streams",
	} {
		if !got[name] {
			t.Errorf("metric %q was not registered", name)
		}
	}
}
