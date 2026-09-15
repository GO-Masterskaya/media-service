package interceptors

import (
	"context"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryInterceptor перехватывает панику в unary RPC и возвращает INTERNAL.
func RecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (_ any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error(
					"panic recovered",
					"method", info.FullMethod,
					"panic", r,
					"stack", string(debug.Stack()),
				)

				err = status.Error(codes.Internal, "internal error")
			}
		}()

		return handler(ctx, req)
	}
}

// RecoveryStreamInterceptor — аналог для streaming RPC.
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error(
					"panic recovered",
					"method", info.FullMethod,
					"panic", r,
					"stack", string(debug.Stack()),
				)

				err = status.Error(codes.Internal, "internal error")
			}
		}()

		return handler(srv, stream)
	}
}
