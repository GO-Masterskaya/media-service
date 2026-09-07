// надо проверить
// Panic не вылетел наружу, получили codes.Internal
// для stream проверить, что recovery ловит панику хендлера,
// и срабатывает stream limiter release

package interceptors

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryUnaryInterceptor_RecoversPanic(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		panic("something went wrong")
	}

	resp, err := RecoveryUnaryInterceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if resp != nil {
		t.Fatalf("expected nil response, got %v", resp)
	}

	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", status.Code(err))
	}
}

func TestRecoveryUnaryInterceptor_PassesHandlerError(t *testing.T) {
	wantErr := errors.New("handler error")

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, wantErr
	}

	_, err := RecoveryUnaryInterceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Test",
		},
		handler,
	)

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
}
