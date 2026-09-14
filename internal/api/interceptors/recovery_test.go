package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryInterceptor(t *testing.T) {
	t.Parallel()

	ic := RecoveryInterceptor()

	_, err := ic(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/media.v1.MediaService/GetMedia",
		},
		func(context.Context, any) (any, error) {
			panic("boom")
		},
	)

	if status.Code(err) != codes.Internal {
		t.Fatalf(
			"got code %v, want Internal",
			status.Code(err),
		)
	}
}
