// надо проверить
// request counter увеличивается
//code записывается
// duration появляется
// active streams +1, после завершения -1

// удалит потом это всё
package metrics

import (
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestRPCRequestsTotal(t *testing.T) {
	RPCRequestsTotal.Reset()

	RPCRequestsTotal.WithLabelValues("/media.v1.MediaService/GetMedia", "OK").Inc()
	RPCRequestsTotal.WithLabelValues("/media.v1.MediaService/GetMedia", "OK").Inc()
	RPCRequestsTotal.WithLabelValues("/media.v1.MediaService/GetMedia", "NotFound").Inc()

	got := testutil.ToFloat64(
		RPCRequestsTotal.WithLabelValues("/media.v1.MediaService/GetMedia", "OK"),
	)

	if got != 2 {
		t.Errorf("expected 2 requests, got %v", got)
	}
}

func TestRPCDuration(t *testing.T) {
	RPCDuration.Reset()

	method := "/media.v1.MediaService/GetMedia"

	RPCDuration.WithLabelValues(method).Observe(0.1)
	RPCDuration.WithLabelValues(method).Observe(0.2)

	ch := make(chan prometheus.Metric, 1)
	RPCDuration.Collect(ch)

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
	// тут сравниваем значение float64, а оно будет типа 0.30000000000000004, поэтому зазор в одну миллиардную
	if math.Abs(histogram.GetSampleSum()-0.3) > 1e-9 {
		t.Errorf(
			"expected duration sum approximately 0.3, got %v",
			histogram.GetSampleSum(),
		)
	}
}

func TestActiveStreams(t *testing.T) {
	// Приводим gauge к известному состоянию.
	current := testutil.ToFloat64(ActiveStreams)
	ActiveStreams.Sub(current)

	ActiveStreams.Inc()
	ActiveStreams.Inc()

	if got := testutil.ToFloat64(ActiveStreams); got != 2 {
		t.Errorf("expected 2 active streams, got %v", got)
	}

	ActiveStreams.Dec()

	if got := testutil.ToFloat64(ActiveStreams); got != 1 {
		t.Errorf("expected 1 active stream, got %v", got)
	}

	ActiveStreams.Dec()

	if got := testutil.ToFloat64(ActiveStreams); got != 0 {
		t.Errorf("expected 0 active streams, got %v", got)
	}
}
