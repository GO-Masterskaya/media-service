package interceptors

import (
	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
)

func UnaryInterceptors(
	authEnabled bool,
	authToken string,
	validator protovalidate.Validator,
) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		RecoveryInterceptor(),
		CorrelationIDInterceptor(),
		InFlightInterceptor(),
		TokenInterceptor(authEnabled, authToken),
		ValidationInterceptor(validator),
	}
}

func StreamInterceptors(
	authEnabled bool,
	authToken string,
	validator protovalidate.Validator,
) []grpc.StreamServerInterceptor {
	return []grpc.StreamServerInterceptor{
		RecoveryStreamInterceptor(),
		CorrelationIDStreamInterceptor(),
		InFlightStreamInterceptor(),
		TokenStreamInterceptor(authEnabled, authToken),
		ValidationStreamInterceptor(validator),
	}
}
