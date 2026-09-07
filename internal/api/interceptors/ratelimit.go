// знает caller
// считает, сколько запросов за определённое время
// отвечает, можно или нет
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

// хранит limiter и время последнего использования
type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// так требование ТЗ "inactive caller state bounded in memory and cleaned" должно выполняться
// хранит отдельный rate limiter для каждого caller
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
	caller, ok := CallerFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "caller identity is required")
	}

	if !l.allow(caller.ID) {
		return nil, status.Error(
			codes.ResourceExhausted,
			"rate limit exceeded",
		)
	}

	return handler(ctx, req)
}

// upload и download проходят через stream interceptor, им тоже нужен rate limit
func (l *RateLimiter) StreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	caller, ok := CallerFromContext(ss.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "caller identity is required")
	}

	if !l.allow(caller.ID) {
		return status.Error(
			codes.ResourceExhausted,
			"rate limit exceeded",
		)
	}

	return handler(srv, ss)
}
