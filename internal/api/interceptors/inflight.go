package interceptors

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
)

var inFlightRPCs atomic.Int64

// InFlightRPCs возвращает число активных non-health RPC.
func InFlightRPCs() int64 {
	return inFlightRPCs.Load()
}

func trackInFlightRPC() func() {
	inFlightRPCs.Add(1)

	return func() {
		inFlightRPCs.Add(-1)
	}
}

// InFlightInterceptor считает активные unary RPC.
// Health RPC в счётчик не входят.
func InFlightInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		defer trackInFlightRPC()()

		return handler(ctx, req)
	}
}

// InFlightStreamInterceptor считает активные streaming RPC.
// Health RPC в счётчик не входят.
func InFlightStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if isHealthMethod(info.FullMethod) {
			return handler(srv, stream)
		}

		defer trackInFlightRPC()()

		return handler(srv, stream)
	}
}
