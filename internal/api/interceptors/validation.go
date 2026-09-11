package interceptors

import (
	"context"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ValidationInterceptor проверяет buf.validate правила до handler.
func ValidationInterceptor(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
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
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		wrapped := &validatedStream{
			ServerStream: stream,
			validator:    v,
		}

		return handler(srv, wrapped)
	}
}
