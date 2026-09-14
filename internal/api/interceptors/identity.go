package interceptors

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	callerIDMetadataKey = "x-caller-id"
	fallbackCallerID    = "authenticated"
)

// Caller отвечает на вопрос: кто вызывает RPC.
type Caller struct {
	ID string
}

type callerKey struct{}

// callerServerStream позволяет заменить Context() у streaming RPC.
type callerServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func WithCaller(ctx context.Context, caller Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

func CallerFromContext(ctx context.Context) (Caller, bool) {
	caller, ok := ctx.Value(callerKey{}).(Caller)
	return caller, ok
}

func NewCallerAllowlist(ids []string) map[string]struct{} {
	allowed := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}

		allowed[id] = struct{}{}
	}

	return allowed
}

// callerFromMetadata определяет caller для текущего RPC.
//
// Если x-caller-id присутствует и не пустой — используем его.
// Если caller-id отсутствует — используем стабильный fallback,
// чтобы запрос не отклонялся только из-за отсутствия metadata.
func callerFromMetadata(
	ctx context.Context,
	allowed map[string]struct{},
) Caller {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Caller{ID: fallbackCallerID}
	}

	values := md.Get(callerIDMetadataKey)
	if len(values) == 0 {
		return Caller{ID: fallbackCallerID}
	}

	callerID := strings.TrimSpace(values[0])
	if callerID == "" {
		return Caller{ID: fallbackCallerID}
	}

	if _, ok := allowed[callerID]; !ok {
		slog.WarnContext(
			ctx,
			"unknown caller ID, using fallback bucket",
			"caller_id", callerID,
		)

		return Caller{ID: fallbackCallerID}
	}

	return Caller{ID: callerID}
}

func CallerUnaryInterceptor(
	allowed map[string]struct{},
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		caller := callerFromMetadata(ctx, allowed)

		ctx = WithCaller(ctx, caller)

		return handler(ctx, req)
	}
}

func CallerStreamInterceptor(
	allowed map[string]struct{},
) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		caller := callerFromMetadata(
			stream.Context(),
			allowed,
		)

		ctx := WithCaller(
			stream.Context(),
			caller,
		)

		wrapped := &callerServerStream{
			ServerStream: stream,
			ctx:          ctx,
		}

		return handler(srv, wrapped)
	}
}

func (s *callerServerStream) Context() context.Context {
	return s.ctx
}
