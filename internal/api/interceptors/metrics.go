package interceptors

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"mediaservice/internal/metrics"
)

func MetricsUnaryInterceptor(
	m *metrics.GRPCMetrics,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		m.RPCRequestsTotal.
			WithLabelValues(
				info.FullMethod,
				status.Code(err).String(),
			).
			Inc()

		m.RPCDuration.
			WithLabelValues(info.FullMethod).
			Observe(time.Since(start).Seconds())

		return resp, err
	}
}

func MetricsStreamInterceptor(
	m *metrics.GRPCMetrics,
) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		m.ActiveStreams.Inc()
		defer m.ActiveStreams.Dec()

		err := handler(srv, stream)

		m.RPCRequestsTotal.
			WithLabelValues(
				info.FullMethod,
				status.Code(err).String(),
			).
			Inc()

		m.RPCDuration.
			WithLabelValues(info.FullMethod).
			Observe(time.Since(start).Seconds())

		return err
	}
}
