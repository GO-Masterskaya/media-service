package interceptors

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestCorrelationIDInterceptor(t *testing.T) {
	t.Parallel()

	ic := CorrelationIDInterceptor()

	var gotCID string

	handler := func(
		ctx context.Context,
		req any,
	) (any, error) {
		gotCID = CorrelationIDFromContext(ctx)
		return nil, nil
	}

	t.Run("propagates incoming id", func(t *testing.T) {
		const cid = "req-123"

		ctx := metadata.NewIncomingContext(
			context.Background(),
			metadata.Pairs(
				"x-correlation-id",
				cid,
			),
		)

		_, err := ic(
			ctx,
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			handler,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if gotCID != cid {
			t.Fatalf(
				"got %q, want %q",
				gotCID,
				cid,
			)
		}
	})

	t.Run("generates uuid when missing", func(t *testing.T) {
		_, err := ic(
			context.Background(),
			nil,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			handler,
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := uuid.Parse(gotCID); err != nil {
			t.Fatalf(
				"expected uuid correlation id, got %q: %v",
				gotCID,
				err,
			)
		}
	})
}
