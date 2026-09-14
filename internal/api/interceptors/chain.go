package interceptors

import (
	"buf.build/go/protovalidate"
	"google.golang.org/grpc"

	"mediaservice/internal/metrics"
)

func UnaryInterceptors(
	authEnabled bool,
	authToken string,
	validator protovalidate.Validator,
	grpcMetrics *metrics.GRPCMetrics,
	rateLimiter *RateLimiter,
	callerAllowlist map[string]struct{},
) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		RecoveryInterceptor(),
		CorrelationIDInterceptor(),
		MetricsUnaryInterceptor(grpcMetrics),
		LoggingUnaryInterceptor,
		InFlightInterceptor(),
		TokenInterceptor(authEnabled, authToken),
		CallerUnaryInterceptor(callerAllowlist),
		rateLimiter.UnaryInterceptor,
		ValidationInterceptor(validator),
	}
}

func StreamInterceptors(
	authEnabled bool,
	authToken string,
	validator protovalidate.Validator,
	grpcMetrics *metrics.GRPCMetrics,
	rateLimiter *RateLimiter,
	streamLimiter *StreamLimiter,
	callerAllowlist map[string]struct{},
) []grpc.StreamServerInterceptor {
	return []grpc.StreamServerInterceptor{
		RecoveryStreamInterceptor(),
		CorrelationIDStreamInterceptor(),
		MetricsStreamInterceptor(grpcMetrics),
		LoggingStreamInterceptor,
		InFlightStreamInterceptor(),
		TokenStreamInterceptor(authEnabled, authToken),
		CallerStreamInterceptor(callerAllowlist),
		rateLimiter.StreamInterceptor,
		streamLimiter.Interceptor,
		ValidationStreamInterceptor(validator),
	}
}
