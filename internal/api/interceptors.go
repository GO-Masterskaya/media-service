package api

import (
	"context"
	"log/slog"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type correlationIDKey struct{}
type apiTokenKey struct{}

// RecoveryInterceptor перехватывает панику в unary RPC и возвращает INTERNAL.
func RecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic recovered", "method", info.FullMethod, "panic", r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// RecoveryStreamInterceptor — аналог для streaming RPC.
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic recovered", "method", info.FullMethod, "panic", r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, stream)
	}
}

// CorrelationIDInterceptor извлекает x-correlation-id из metadata или генерирует новый UUID.
// Кладёт значение в context, прокидывает в ответные заголовки и логирует старт/финиш RPC.
func CorrelationIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		cid := extractOrGenerateCID(ctx)
		ctx = context.WithValue(ctx, correlationIDKey{}, cid)
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-correlation-id", cid))

		slog.Info("rpc started", "method", info.FullMethod, "correlation_id", cid)
		resp, err := handler(ctx, req)
		slog.Info("rpc finished", "method", info.FullMethod, "correlation_id", cid, "error", err)
		return resp, err
	}
}

// CorrelationIDStreamInterceptor — для streaming RPC.
func CorrelationIDStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		cid := extractOrGenerateCID(stream.Context())
		ctx := context.WithValue(stream.Context(), correlationIDKey{}, cid)
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-correlation-id", cid))

		wrapped := &ctxStream{ServerStream: stream, ctx: ctx}
		slog.Info("stream started", "method", info.FullMethod, "correlation_id", cid)
		err := handler(srv, wrapped)
		slog.Info("stream finished", "method", info.FullMethod, "correlation_id", cid, "error", err)
		return err
	}
}

// TokenInterceptor читает authorization из metadata, кладёт в context, не валидирует.
func TokenInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		token := extractToken(ctx)
		ctx = context.WithValue(ctx, apiTokenKey{}, token)
		return handler(ctx, req)
	}
}

// TokenStreamInterceptor — для streaming RPC.
func TokenStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		token := extractToken(stream.Context())
		ctx := context.WithValue(stream.Context(), apiTokenKey{}, token)
		wrapped := &ctxStream{ServerStream: stream, ctx: ctx}
		return handler(srv, wrapped)
	}
}

// ValidationInterceptor проверяет buf.validate правила до handler.
func ValidationInterceptor(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if msg, ok := req.(protoreflect.ProtoMessage); ok {
			if err := v.Validate(msg); err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
		}
		return handler(ctx, req)
	}
}

// ValidationStreamInterceptor валидирует каждое входящее сообщение стрима.
func ValidationStreamInterceptor(v protovalidate.Validator) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := &validatedStream{ServerStream: stream, validator: v}
		return handler(srv, wrapped)
	}
}