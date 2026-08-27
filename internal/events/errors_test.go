package events

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestClassifyErrorExhaustive проверяет, что все gRPC коды ошибок покрыты
// и ClassifyError не паникует на известных статусах.
func TestClassifyErrorExhaustive(t *testing.T) {
	// codes.OK исключён: status.Error(codes.OK, ...) возвращает nil,
	// а ClassifyError(nil) — nil без паники. OK — не код ошибки.
	allCodes := []codes.Code{
		codes.Canceled,
		codes.Unknown,
		codes.InvalidArgument,
		codes.DeadlineExceeded,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss,
		codes.Unauthenticated,
	}
	for _, code := range allCodes {
		t.Run(code.String(), func(t *testing.T) {
			err := status.Error(code, "test")
			assert.NotPanics(t, func() { ClassifyError(err) })
		})
	}
}

func TestClassifyError_OK_IsNil(t *testing.T) {
	err := status.Error(codes.OK, "test")
	assert.NoError(t, err) // status.Error(codes.OK, ...) == nil
	assert.Equal(t, nil, ClassifyError(err))
}

func TestClassifyError_AlreadyWrapped(t *testing.T) {
	pe := PermanentError{errors.New("perm")}
	re := RetryableError{errors.New("retry")}

	assert.True(t, IsPermanent(ClassifyError(pe)))
	assert.True(t, IsRetryable(ClassifyError(re)))
}

func TestClassifyError_ContextCanceled(t *testing.T) {
	err := context.Canceled
	classified := ClassifyError(err)
	assert.True(t, IsPermanent(classified))
}

func TestClassifyError_NonGRPC_DefaultRetryable(t *testing.T) {
	err := errors.New("random failure")
	classified := ClassifyError(err)
	assert.True(t, IsRetryable(classified))
}
