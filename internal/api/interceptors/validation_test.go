package interceptors

import (
	"context"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mediav1 "mediaservice/proto/media/v1"
)

func TestValidationInterceptor(t *testing.T) {
	t.Parallel()

	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("validator: %v", err)
	}

	ic := ValidationInterceptor(v)

	t.Run("rejects invalid request", func(t *testing.T) {
		req := &mediav1.GetMediaRequest{
			MediaId: "not-a-uuid",
		}

		_, err := ic(
			context.Background(),
			req,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			func(context.Context, any) (any, error) {
				t.Fatal("handler must not be called")
				return nil, nil
			},
		)

		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf(
				"got code %v, want InvalidArgument",
				status.Code(err),
			)
		}
	})

	t.Run("accepts valid request", func(t *testing.T) {
		req := &mediav1.GetMediaRequest{
			MediaId: uuid.NewString(),
		}

		_, err := ic(
			context.Background(),
			req,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetMedia",
			},
			func(context.Context, any) (any, error) {
				return "ok", nil
			},
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("allows empty variant", func(t *testing.T) {
		req := &mediav1.GetDownloadURLRequest{
			MediaId: uuid.NewString(),
			Variant: "",
		}

		_, err := ic(
			context.Background(),
			req,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/GetDownloadURL",
			},
			func(context.Context, any) (any, error) {
				return "ok", nil
			},
		)

		if err != nil {
			t.Fatalf(
				"empty variant must be allowed: %v",
				err,
			)
		}
	})

	t.Run("allows zero page_size", func(t *testing.T) {
		req := &mediav1.ListMediaByOwnerRequest{
			OwnerId:  uuid.NewString(),
			PageSize: 0,
		}

		_, err := ic(
			context.Background(),
			req,
			&grpc.UnaryServerInfo{
				FullMethod: "/media.v1.MediaService/ListMediaByOwner",
			},
			func(context.Context, any) (any, error) {
				return "ok", nil
			},
		)

		if err != nil {
			t.Fatalf(
				"zero page_size must be allowed: %v",
				err,
			)
		}
	})
}
