// logging знает только что RPC начался и закончился, поэтому ему нужны
// caller
// method
// duration
// status
// отвечает только за запись в лог начала и конца RPC
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

	caller, _ := CallerFromContext(ctx)

	slog.InfoContext(
		ctx,
		"gRPC request started",
		"method", info.FullMethod,
		"caller", caller.ID,
	)

	resp, err := handler(ctx, req)

	slog.InfoContext(
		ctx,
		"gRPC request finished",
		"method", info.FullMethod,
		"caller", caller.ID,
		"code", status.Code(err).String(),
		// если ошибка есть, получим ResourceExhausted
		"duration", time.Since(start),
	)

	return resp, err
}

func LoggingStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()

	caller, _ := CallerFromContext(ss.Context())

	slog.InfoContext(
		ss.Context(),
		"gRPC stream started",
		"method", info.FullMethod,
		"caller", caller.ID,
	)

	err := handler(srv, ss)

	slog.InfoContext(
		ss.Context(),
		"gRPC stream finished",
		"method", info.FullMethod,
		"caller", caller.ID,
		"code", status.Code(err).String(),
		"duration", time.Since(start),
	)

	return err
}
