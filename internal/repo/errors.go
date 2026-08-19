package repo

import "errors"

var (
	ErrNotFound           = errors.New("not found")
	ErrLeaseMismatch      = errors.New("job lease mismatch")
	ErrInvalidTransition  = errors.New("invalid job status transition")
	ErrConcurrentConflict = errors.New("concurrent modification")
)
