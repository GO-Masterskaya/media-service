package api

import (
	"context"

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
