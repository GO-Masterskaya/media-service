package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type correlationIDKey struct{}

// CorrelationIDInterceptor извлекает x-correlation-id из metadata
// или генерирует новый UUID.
func CorrelationIDInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		cid := extractOrGenerateCID(ctx)

		ctx = context.WithValue(ctx, correlationIDKey{}, cid)

		_ = grpc.SetHeader(
			ctx,
			metadata.Pairs("x-correlation-id", cid),
		)

		return handler(ctx, req)
	}
}

// CorrelationIDStreamInterceptor — аналог для streaming RPC.
func CorrelationIDStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		cid := extractOrGenerateCID(stream.Context())

		ctx := context.WithValue(
			stream.Context(),
			correlationIDKey{},
			cid,
		)

		_ = grpc.SetHeader(
			ctx,
			metadata.Pairs("x-correlation-id", cid),
		)

		wrapped := &ctxStream{
			ServerStream: stream,
			ctx:          ctx,
		}

		return handler(srv, wrapped)
	}
}

// CorrelationIDFromContext возвращает correlation id,
// установленный interceptor'ом.
func CorrelationIDFromContext(ctx context.Context) string {
	cid, _ := ctx.Value(correlationIDKey{}).(string)
	return cid
}
