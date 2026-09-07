// здесь нужно собрать recovery-caller-ratelimit-metrics-logging-handler
// нужны две цепочки: ChainUnaryInterceptor для обычных RPC
// и ChainStreamInterceptor для streaming RPC

package interceptors

import "google.golang.org/grpc"

func UnaryInterceptors(
	rateLimiter *RateLimiter,
) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		RecoveryUnaryInterceptor,
		CallerUnaryInterceptor,
		rateLimiter.UnaryInterceptor,
		MetricsUnaryInterceptor,
		LoggingUnaryInterceptor,
	}
}

func StreamInterceptors(
	rateLimiter *RateLimiter,
	streamLimiter *StreamLimiter,
) []grpc.StreamServerInterceptor {
	return []grpc.StreamServerInterceptor{
		RecoveryStreamInterceptor,
		CallerStreamInterceptor,
		rateLimiter.StreamInterceptor,
		streamLimiter.Interceptor,
		MetricsStreamInterceptor,
		LoggingStreamInterceptor,
	}
}
