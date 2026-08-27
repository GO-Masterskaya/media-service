package repo

import "errors"

var (
	ErrNotFound           = errors.New("not found")
	ErrLeaseMismatch      = errors.New("job lease mismatch")
	ErrInvalidTransition  = errors.New("invalid job status transition")
	ErrConcurrentConflict = errors.New("concurrent modification")
)

// Processed events (Kafka идемпотентность, задача #28)

// ErrFingerprintConflict возвращается Claim(), когда event_id уже встречался
// с другим payload fingerprint. Событие не должно исполняться.
var ErrFingerprintConflict = errors.New("processed event fingerprint conflict")

// ErrClaimHeld возвращается Claim(), когда другой owner держит живой
// (неистёкший) processing lease для этого event_id.
var ErrClaimHeld = errors.New("processed event claim held by another owner")

// ErrClaimLost возвращается MarkDone/MarkDLQ, когда owner больше не держит
// processing claim (lease истёк и был перехвачен другим).
var ErrClaimLost = errors.New("processed event claim lost")
