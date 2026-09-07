// только считает
// количество RPC
// длительность
// gRPC code
// активные  streams
package interceptors

import (
	"context"
	"time"

	"mediaservice/internal/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func MetricsUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	metrics.RPCRequestsTotal.WithLabelValues(
		info.FullMethod,
		status.Code(err).String(),
	).Inc()

	metrics.RPCDuration.WithLabelValues(
		info.FullMethod,
	).Observe(time.Since(start).Seconds())

	return resp, err
}

// метрики для streams
func MetricsStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()

	// active streams наверное лучше увеличивать/уменьшать вокруг handler
	metrics.ActiveStreams.Inc()
	defer metrics.ActiveStreams.Dec()

	err := handler(srv, ss)

	metrics.RPCRequestsTotal.WithLabelValues(
		info.FullMethod,
		status.Code(err).String(),
	).Inc()

	metrics.RPCDuration.WithLabelValues(
		info.FullMethod,
	).Observe(time.Since(start).Seconds())

	return err
}
