package interceptors

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func RecoveryUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	// если внутри хендлера случилась паника, ловим её и превращаем в gRPC ошибку
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(
				ctx,
				"panic recovered",
				"method", info.FullMethod,
			)

			err = status.Error(
				codes.Internal,
				"internal server error",
			)
		}
	}()

	return handler(ctx, req)
}

func RecoveryStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(
				ss.Context(),
				"panic recovered",
				"method", info.FullMethod,
				"panic", r,
			)

			err = status.Error(
				codes.Internal,
				"internal server error",
			)
		}
	}()

	return handler(srv, ss)
}
