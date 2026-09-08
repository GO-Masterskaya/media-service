package processing

import (
	"errors"
	"testing"
)

func TestClassifyHandlerError_Permanent(t *testing.T) {
	err := ClassifyHandlerError(PermanentError{Err: errors.New("bad input")})
	if !IsPermanent(err) {
		t.Fatalf("expected permanent error")
	}
}

func TestClassifyHandlerError_RetryableDefault(t *testing.T) {
	err := ClassifyHandlerError(errors.New("temporary"))
	if IsPermanent(err) {
		t.Fatalf("expected retryable error")
	}
}
