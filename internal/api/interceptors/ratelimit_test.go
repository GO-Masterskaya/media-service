// надо проверить
// один caller  может сделать burst запросов
// следующий запрос получает ResourceExhausted
// другой caller при этом продолжает работать
// limiter действительно отдельный для каждого коллера
// cleanup  удаляет неактивного коллера

package interceptors

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRateLimiter_AllowsBurst(t *testing.T) {
	limiter := NewRateLimiter(0, 2)

	if !limiter.allow("caller-1") {
		t.Fatal("first request should be allowed")
	}

	if !limiter.allow("caller-1") {
		t.Fatal("second request should be allowed")
	}

	if limiter.allow("caller-1") {
		t.Fatal("third request should be rejected")
	}
}

func TestRateLimiter_IsolatedByCaller(t *testing.T) {
	limiter := NewRateLimiter(0, 1)

	if !limiter.allow("caller-1") {
		t.Fatal("caller-1 first request should be allowed")
	}

	if limiter.allow("caller-1") {
		t.Fatal("caller-1 second request should be rejected")
	}

	if !limiter.allow("caller-2") {
		t.Fatal("caller-2 should have its own limit")
	}
}

func TestRateLimiter_UnaryInterceptor(t *testing.T) {
	limiter := NewRateLimiter(0, 1)

	ctx := WithCaller(
		context.Background(),
		Caller{ID: "caller-1"},
	)

	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	_, err := limiter.UnaryInterceptor(
		ctx,
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = limiter.UnaryInterceptor(
		ctx,
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf(
			"expected ResourceExhausted, got %v",
			status.Code(err),
		)
	}
}

func TestRateLimiter_Cleanup(t *testing.T) {
	limiter := NewRateLimiter(1, 1)

	limiter.allow("inactive-caller")

	limiter.mu.Lock()
	limiter.limiters["inactive-caller"].lastUsed =
		time.Now().Add(-time.Hour)
	limiter.mu.Unlock()

	limiter.cleanup(time.Minute)

	limiter.mu.Lock()
	_, exists := limiter.limiters["inactive-caller"]
	limiter.mu.Unlock()

	if exists {
		t.Fatal("inactive caller should have been removed")
	}
}
