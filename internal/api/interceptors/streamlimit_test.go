// надо проверить
// количество стримов выше лимита даёт Resource exhausted
// после освобождения места пускает новый стрим
// елси caller занял доступные ему стримы, другой caller всё равно может открыть стрим

package interceptors

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testServerStream struct {
	ctx context.Context
}

func (s *testServerStream) Context() context.Context {
	return s.ctx
}

func (s *testServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *testServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *testServerStream) SetTrailer(metadata.MD) {}

func (s *testServerStream) SendMsg(any) error {
	return nil
}

func (s *testServerStream) RecvMsg(any) error {
	return nil
}

func TestStreamLimiter_AllowsUpToLimit(t *testing.T) {
	limiter := NewStreamLimiter(2)

	if !limiter.acquire("caller-1") {
		t.Fatal("first stream should be allowed")
	}

	if !limiter.acquire("caller-1") {
		t.Fatal("second stream should be allowed")
	}

	if limiter.acquire("caller-1") {
		t.Fatal("third stream should be rejected")
	}
}

func TestStreamLimiter_IsolatedByCaller(t *testing.T) {
	limiter := NewStreamLimiter(1)

	if !limiter.acquire("caller-1") {
		t.Fatal("caller-1 should be allowed")
	}

	if limiter.acquire("caller-1") {
		t.Fatal("second caller-1 stream should be rejected")
	}

	if !limiter.acquire("caller-2") {
		t.Fatal("caller-2 should have its own limit")
	}
}

func TestStreamLimiter_Release(t *testing.T) {
	limiter := NewStreamLimiter(1)

	if !limiter.acquire("caller-1") {
		t.Fatal("stream should be acquired")
	}

	limiter.release("caller-1")

	if !limiter.acquire("caller-1") {
		t.Fatal("stream should be available after release")
	}
}

func TestStreamLimiter_ReleasesAfterHandlerError(t *testing.T) {
	limiter := NewStreamLimiter(1)

	ctx := WithCaller(
		context.Background(),
		Caller{ID: "caller-1"},
	)

	stream := &testServerStream{
		ctx: ctx,
	}

	wantErr := errors.New("handler failed")

	handler := func(
		srv any,
		stream grpc.ServerStream,
	) error {
		return wantErr
	}

	err := limiter.Interceptor(
		nil,
		stream,
		&grpc.StreamServerInfo{
			FullMethod: "/test.Service/DownloadStream",
		},
		handler,
	)

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected handler error, got %v", err)
	}

	// Если defer release() сработал,
	// тот же caller снова может открыть stream.
	if !limiter.acquire("caller-1") {
		t.Fatal("stream slot should have been released")
	}
}

func TestStreamLimiter_RejectsWhenLimitExceeded(t *testing.T) {
	limiter := NewStreamLimiter(1)

	ctx := WithCaller(
		context.Background(),
		Caller{ID: "caller-1"},
	)

	stream := &testServerStream{
		ctx: ctx,
	}

	handler := func(
		srv any,
		stream grpc.ServerStream,
	) error {
		t.Fatal("handler should not be called")
		return nil
	}

	// Занимаем единственный слот.
	if !limiter.acquire("caller-1") {
		t.Fatal("failed to acquire first stream")
	}

	err := limiter.Interceptor(
		nil,
		stream,
		&grpc.StreamServerInfo{
			FullMethod: "/test.Service/DownloadStream",
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
