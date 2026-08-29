package repo

import "errors"

var (
	ErrNotFound          = errors.New("not found")
	ErrLeaseMismatch     = errors.New("job lease mismatch")
	ErrInvalidTransition = errors.New("invalid job status transition")

	ErrOwnerMismatch      = errors.New("owner mismatch")
	ErrConcurrentConflict = errors.New("concurrent modification") // unique (owner, idempotency_key)
	ErrIDConflict         = errors.New("media id conflict")       // PK media.id

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

// ErrMediaDeleting возвращается CreateAttachment, если media находится
// в статусе deleting. Это бизнес-ошибка, а не инфраструктурная:
// повторная попытка (retry) не изменит состояние записи.
// Service/handler должен мапить её в codes.FailedPrecondition,
// а events.ClassifyError — в PermanentError (сразу DLQ, без retry).
var ErrMediaDeleting = errors.New("media is being deleted")
