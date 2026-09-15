package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestCallerUnaryInterceptor_AllowedCaller(t *testing.T) {
	allowed := NewCallerAllowlist([]string{
		"astro-backend",
		"crm",
	})

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
			t.Fatalf(
				"expected caller astro-backend, got %q",
				caller.ID,
			)
		}

		return "ok", nil
	}

	resp, err := CallerUnaryInterceptor(allowed)(
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

func TestCallerUnaryInterceptor_MissingCallerUsesFallback(t *testing.T) {
	allowed := NewCallerAllowlist([]string{"astro-backend"})

	handler := func(ctx context.Context, req any) (any, error) {
		caller, ok := CallerFromContext(ctx)
		if !ok {
			t.Fatal("caller not found in context")
		}

		if caller.ID != fallbackCallerID {
			t.Fatalf(
				"expected fallback caller %q, got %q",
				fallbackCallerID,
				caller.ID,
			)
		}

		return "ok", nil
	}

	_, err := CallerUnaryInterceptor(allowed)(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallerUnaryInterceptor_UnknownCallerUsesFallback(t *testing.T) {
	allowed := NewCallerAllowlist([]string{
		"astro-backend",
		"crm",
	})

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-caller-id",
			"random-client",
		),
	)

	handler := func(ctx context.Context, req any) (any, error) {
		caller, ok := CallerFromContext(ctx)
		if !ok {
			t.Fatal("caller not found in context")
		}

		if caller.ID != fallbackCallerID {
			t.Fatalf(
				"expected fallback caller %q, got %q",
				fallbackCallerID,
				caller.ID,
			)
		}

		return nil, nil
	}

	_, err := CallerUnaryInterceptor(allowed)(
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
}

func TestCallerUnaryInterceptor_EmptyCallerUsesFallback(t *testing.T) {
	allowed := NewCallerAllowlist([]string{"astro-backend"})

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			"x-caller-id",
			"",
		),
	)

	handler := func(ctx context.Context, req any) (any, error) {
		caller, ok := CallerFromContext(ctx)
		if !ok {
			t.Fatal("caller not found in context")
		}

		if caller.ID != fallbackCallerID {
			t.Fatalf(
				"expected fallback caller %q, got %q",
				fallbackCallerID,
				caller.ID,
			)
		}

		return nil, nil
	}

	_, err := CallerUnaryInterceptor(allowed)(
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
}

func TestCallerStreamInterceptor_AllowedCaller(t *testing.T) {
	allowed := NewCallerAllowlist([]string{"crm"})

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
			t.Fatalf(
				"expected caller crm, got %q",
				caller.ID,
			)
		}

		return nil
	}

	err := CallerStreamInterceptor(allowed)(
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

func TestNewCallerAllowlist_TrimsAndSkipsEmptyValues(t *testing.T) {
	allowed := NewCallerAllowlist([]string{
		" astro-backend ",
		"",
		"   ",
		"crm",
	})

	if _, ok := allowed["astro-backend"]; !ok {
		t.Fatal("astro-backend should be allowed")
	}

	if _, ok := allowed["crm"]; !ok {
		t.Fatal("crm should be allowed")
	}

	if _, ok := allowed[""]; ok {
		t.Fatal("empty caller ID must not be added")
	}
}
