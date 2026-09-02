package api

import (
	"context"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mediav1 "mediaservice/proto/media/v1"
)

func TestTokenInterceptor(t *testing.T) {
	t.Parallel()

	const token = "test-secret"
	ic := TokenInterceptor(token)
	okHandler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	t.Run("valid bearer token", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
		resp, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"}, okHandler)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp != "ok" {
			t.Fatalf("got %v, want ok", resp)
		}
	})

	t.Run("valid raw token", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
		_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"}, okHandler)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("missing token", func(t *testing.T) {
		_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"}, okHandler)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got code %v, want Unauthenticated", status.Code(err))
		}
	})

	t.Run("wrong token", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
		_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"}, okHandler)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got code %v, want Unauthenticated", status.Code(err))
		}
	})

	t.Run("health check bypass", func(t *testing.T) {
		_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, okHandler)
		if err != nil {
			t.Fatalf("health check should bypass auth: %v", err)
		}
	})
}

func TestCorrelationIDInterceptor(t *testing.T) {
	t.Parallel()

	ic := CorrelationIDInterceptor()
	var gotCID string
	handler := func(ctx context.Context, req any) (any, error) {
		gotCID = CorrelationIDFromContext(ctx)
		return nil, nil
	}

	t.Run("propagates incoming id", func(t *testing.T) {
		const cid = "req-123"
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-correlation-id", cid))
		if _, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"}, handler); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotCID != cid {
			t.Fatalf("got %q, want %q", gotCID, cid)
		}
	})

	t.Run("generates uuid when missing", func(t *testing.T) {
		if _, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"}, handler); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := uuid.Parse(gotCID); err != nil {
			t.Fatalf("expected uuid correlation id, got %q: %v", gotCID, err)
		}
	})
}

func TestRecoveryInterceptor(t *testing.T) {
	t.Parallel()

	ic := RecoveryInterceptor()
	_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"},
		func(context.Context, any) (any, error) {
			panic("boom")
		},
	)
	if status.Code(err) != codes.Internal {
		t.Fatalf("got code %v, want Internal", status.Code(err))
	}
}

func TestValidationInterceptor(t *testing.T) {
	t.Parallel()

	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("validator: %v", err)
	}
	ic := ValidationInterceptor(v)

	t.Run("rejects invalid request", func(t *testing.T) {
		req := &mediav1.GetMediaRequest{MediaId: "not-a-uuid"}
		_, err := ic(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"},
			func(context.Context, any) (any, error) {
				t.Fatal("handler must not be called")
				return nil, nil
			},
		)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got code %v, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("accepts valid request", func(t *testing.T) {
		req := &mediav1.GetMediaRequest{MediaId: uuid.NewString()}
		_, err := ic(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: "/media.v1.MediaService/GetMedia"},
			func(context.Context, any) (any, error) { return "ok", nil },
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
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
