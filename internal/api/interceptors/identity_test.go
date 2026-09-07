//надо проверить
// если x-caller-id есть, caller попал в контекст
// если x-caller-id отсутствует, то Unauthenticated
// если x-caller-id пустой, то ошибка
// разные caller не смешиваются

package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCallerUnaryInterceptor(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-caller-id",
			"astro-backend",
		),
	)

	handler := func(ctx context.Context, req any) (any, error) {
		caller, ok := CallerFromContext(ctx)

		if !ok {
			t.Fatal("caller not found in context")
		}

		if caller.ID != "astro-backend" {
			t.Fatalf("expected caller astro-backend, got %q", caller.ID)
		}

		return "ok", nil
	}

	resp, err := CallerUnaryInterceptor(
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

	if resp != "ok" {
		t.Fatalf("expected ok response, got %v", resp)
	}
}

func TestCallerUnaryInterceptor_MissingCaller(t *testing.T) {
	ctx := context.Background()

	handler := func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	}

	_, err := CallerUnaryInterceptor(
		ctx,
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf(
			"expected Unauthenticated, got %v",
			status.Code(err),
		)
	}
}

func TestCallerUnaryInterceptor_EmptyCaller(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-caller-id", ""),
	)

	handler := func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	}

	_, err := CallerUnaryInterceptor(
		ctx,
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf(
			"expected Unauthenticated, got %v",
			status.Code(err),
		)
	}
}

func TestCallerStreamInterceptor(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-caller-id",
			"crm",
		),
	)

	stream := &testServerStream{
		ctx: ctx,
	}

	handler := func(
		srv any,
		ss grpc.ServerStream,
	) error {
		caller, ok := CallerFromContext(ss.Context())

		if !ok {
			t.Fatal("caller not found in stream context")
		}

		if caller.ID != "crm" {
			t.Fatalf("expected caller crm, got %q", caller.ID)
		}

		return nil
	}

	err := CallerStreamInterceptor(
		nil,
		stream,
		&grpc.StreamServerInfo{
			FullMethod: "/test.Service/DownloadStream",
		},
		handler,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
