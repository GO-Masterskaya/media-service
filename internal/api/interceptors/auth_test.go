package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestTokenInterceptor(t *testing.T) {
	t.Parallel()

	const token = "test-secret"

	ic := TokenInterceptor(true, token)

	okHandler := func(
		ctx context.Context,
		req any,
	) (any, error) {
		return "ok", nil
	}

	t.Run("valid bearer token", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				"Bearer "+token,
			),
		)

		resp, err := ic(
			ctx,
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			okHandler,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resp != "ok" {
			t.Fatalf("got %v, want ok", resp)
		}
	})

	t.Run("valid raw token", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				token,
			),
		)

		_, err := ic(
			ctx,
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			okHandler,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("missing token", func(t *testing.T) {
		_, err := ic(
			context.Background(),
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			okHandler,
		)

		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf(
				"got code %v, want Unauthenticated",
				status.Code(err),
			)
		}
	})

	t.Run("wrong token", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				"Bearer wrong",
			),
		)

		_, err := ic(
			ctx,
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			okHandler,
		)

		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf(
				"got code %v, want Unauthenticated",
				status.Code(err),
			)
		}
	})

	t.Run("health check bypass", func(t *testing.T) {
		_, err := ic(
			context.Background(),
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/grpc.health.v1.Health/Check",
			},
			okHandler,
		)

		if err != nil {
			t.Fatalf(
				"health check should bypass auth: %v",
				err,
			)
		}
	})

	t.Run("disabled skips auth", func(t *testing.T) {
		disabled := TokenInterceptor(false, token)

		_, err := disabled(
			context.Background(),
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			okHandler,
		)

		if err != nil {
			t.Fatalf(
				"disabled auth should allow request: %v",
				err,
			)
		}
	})
}

func TestTokenMatches(t *testing.T) {
	t.Parallel()

	if !tokenMatches("Bearer secret", "secret") {
		t.Fatal("expected bearer match")
	}

	if tokenMatches("Bearer wrong", "secret") {
		t.Fatal("expected mismatch")
	}

	if tokenMatches("", "secret") {
		t.Fatal("empty header must not match")
	}
}
