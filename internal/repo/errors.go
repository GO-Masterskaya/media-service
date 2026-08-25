package repo

import "errors"

var (
	ErrNotFound           = errors.New("not found")
	ErrLeaseMismatch      = errors.New("job lease mismatch")
	ErrInvalidTransition  = errors.New("invalid job status transition")
	ErrConcurrentConflict = errors.New("concurrent modification")
	ErrOwnerMismatch      = errors.New("owner mismatch")
)

// Processed events (Kafka идемпотентность, задача #28)

var ErrFingerprintConflict = errors.New("processed event fingerprint conflict")
var ErrClaimHeld = errors.New("processed event claim held by another owner")
var ErrClaimLost = errors.New("processed event claim lost")
var ErrMediaDeleting = errors.New("media is being deleted")
