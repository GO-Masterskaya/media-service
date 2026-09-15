package interceptors

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func LoggingUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()

	slog.InfoContext(
		ctx,
		"gRPC request started",
		logAttrsWithCorrelationID(
			ctx,
			"method", info.FullMethod,
		)...,
	)

	resp, err := handler(ctx, req)

	slog.InfoContext(
		ctx,
		"gRPC request finished",
		logAttrsWithCorrelationID(
			ctx,
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration", time.Since(start),
		)...,
	)

	return resp, err
}

func LoggingStreamInterceptor(
	srv any,
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	ctx := stream.Context()

	slog.InfoContext(
		ctx,
		"gRPC stream started",
		logAttrsWithCorrelationID(
			ctx,
			"method", info.FullMethod,
		)...,
	)

	err := handler(srv, stream)

	slog.InfoContext(
		ctx,
		"gRPC stream finished",
		logAttrsWithCorrelationID(
			ctx,
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration", time.Since(start),
		)...,
	)

	return err
}

func logAttrsWithCorrelationID(ctx context.Context, attrs ...any) []any {
	if cid := CorrelationIDFromContext(ctx); cid != "" {
		return append(
			[]any{"correlation_id", cid},
			attrs...,
		)
	}

	return attrs
}
