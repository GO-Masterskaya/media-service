package api

import (
	"context"
	"crypto/subtle"
	"strings"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func extractOrGenerateCID(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if vals := md.Get("x-correlation-id"); len(vals) > 0 && vals[0] != "" {
		return vals[0]
	}
	return uuid.New().String()
}

func extractToken(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if vals := md.Get("authorization"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func tokenMatches(header, expected string) bool {
	if expected == "" || header == "" {
		return false
	}
	token := strings.TrimSpace(header)
	const prefix = "Bearer "
	if strings.HasPrefix(token, prefix) {
		token = strings.TrimSpace(token[len(prefix):])
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

// CorrelationIDFromContext возвращает correlation id, установленный interceptor'ом.
func CorrelationIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return v
	}
	return ""
}

// logAttrs добавляет correlation_id к slog-атрибутам, если он есть в context.
func logAttrs(ctx context.Context, attrs ...any) []any {
	if cid := CorrelationIDFromContext(ctx); cid != "" {
		attrs = append([]any{"correlation_id", cid}, attrs...)
	}
	return attrs
}

// ctxStream заменяет Context() у ServerStream.
type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }

// validatedStream валидирует каждое RecvMsg.
type validatedStream struct {
	grpc.ServerStream
	validator protovalidate.Validator
}

func (s *validatedStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if msg, ok := m.(protoreflect.ProtoMessage); ok {
		if err := s.validator.Validate(msg); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return nil
}
