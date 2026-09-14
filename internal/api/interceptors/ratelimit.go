package interceptors

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// RateLimiter хранит отдельный token bucket для каждого caller.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rateLimiterEntry

	rate  rate.Limit
	burst int
}

func NewRateLimiter(r rate.Limit, burst int) *RateLimiter {
	return &RateLimiter{
		limiters: make(map[string]*rateLimiterEntry),
		rate:     r,
		burst:    burst,
	}
}

func (l *RateLimiter) allow(callerID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.limiters[callerID]
	if !ok {
		entry = &rateLimiterEntry{
			limiter: rate.NewLimiter(l.rate, l.burst),
		}

		l.limiters[callerID] = entry
	}

	entry.lastUsed = time.Now()

	return entry.limiter.Allow()
}

func (l *RateLimiter) UnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if isHealthMethod(info.FullMethod) {
		return handler(ctx, req)
	}

	caller, ok := CallerFromContext(ctx)
	if !ok {
		return nil, status.Error(
			codes.Internal,
			"caller identity missing from context",
		)
	}

	if !l.allow(caller.ID) {
		return nil, status.Error(
			codes.ResourceExhausted,
			"rate limit exceeded",
		)
	}

	return handler(ctx, req)
}

func (l *RateLimiter) StreamInterceptor(
	srv any,
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if isHealthMethod(info.FullMethod) {
		return handler(srv, stream)
	}

	caller, ok := CallerFromContext(stream.Context())
	if !ok {
		return status.Error(
			codes.Internal,
			"caller identity missing from context",
		)
	}

	if !l.allow(caller.ID) {
		return status.Error(
			codes.ResourceExhausted,
			"rate limit exceeded",
		)
	}

	return handler(srv, stream)
}
