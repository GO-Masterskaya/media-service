package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TokenInterceptor проверяет authorization против ожидаемого токена, если enabled.
// Health RPC пропускаются без проверки. При enabled=false — no-op.
func TokenInterceptor(enabled bool, expectedToken string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if !enabled || isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		token := extractToken(ctx)
		if !tokenMatches(token, expectedToken) {
			return nil, status.Error(
				codes.Unauthenticated,
				"invalid or missing authorization token",
			)
		}

		return handler(ctx, req)
	}
}

// TokenStreamInterceptor — для streaming RPC.
func TokenStreamInterceptor(enabled bool, expectedToken string) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if !enabled || isHealthMethod(info.FullMethod) {
			return handler(srv, stream)
		}

		token := extractToken(stream.Context())
		if !tokenMatches(token, expectedToken) {
			return status.Error(
				codes.Unauthenticated,
				"invalid or missing authorization token",
			)
		}

		return handler(srv, stream)
	}
}
