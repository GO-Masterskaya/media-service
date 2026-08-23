package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
)

// DLQPublisher отправляет события в dead-letter topic.
type DLQPublisher interface {
	Publish(ctx context.Context, original []byte, eventID uuid.UUID, reason string) error
}

type Handler struct {
	mediaSvc      MediaService
	eventRepo     repo.ProcessedEventRepo
	dlq           DLQPublisher
	consumerID    string
	leaseDuration time.Duration
	log           *slog.Logger
}

func NewHandler(
	mediaSvc MediaService,
	eventRepo repo.ProcessedEventRepo,
	dlq DLQPublisher,
	consumerID string,
	log *slog.Logger,
) *Handler {
	if log == nil {
		log = slog.Default()
	}
	if consumerID == "" {
		consumerID = "unknown"
	}
	return &Handler{
		mediaSvc:      mediaSvc,
		eventRepo:     eventRepo,
		dlq:           dlq,
		consumerID:    consumerID,
		leaseDuration: 30 * time.Second,
		log:           log,
	}
}

// Result — исход обработки. Committable=true → offset можно фиксировать.
type Result struct {
	Committable bool
	EventID     uuid.UUID
	Error       error
}

// Handle — точка входа из Kafka consumer (#27).
func (h *Handler) Handle(ctx context.Context, raw []byte) Result {
	env, err := DecodeEnvelope(raw)
	if err != nil {
		h.sendDLQ(ctx, raw, uuid.Nil, "invalid envelope: "+err.Error())
		return Result{Committable: true, Error: err}
	}

	fingerprint := h.fingerprint(raw)
	_, claimed, err := h.eventRepo.Claim(ctx, env.EventID, fingerprint, h.consumerID, h.leaseDuration)
	if err != nil {
		switch {
		case errors.Is(err, repo.ErrFingerprintConflict):
			h.sendDLQ(ctx, raw, env.EventID, "fingerprint conflict")
			return Result{Committable: true, EventID: env.EventID, Error: err}
		case errors.Is(err, repo.ErrClaimHeld):
			return Result{Committable: false, EventID: env.EventID, Error: err}
		default:
			h.log.Error("claim failed", slog.Any("error", err), slog.String("event_id", env.EventID.String()))
			return Result{Committable: false, EventID: env.EventID, Error: RetryableError{err}}
		}
	}

	if !claimed {
		h.log.Info("event already processed, skipping", slog.String("event_id", env.EventID.String()))
		return Result{Committable: true, EventID: env.EventID}
	}

	cmdErr := h.handleOnce(ctx, env)

	if cmdErr == nil {
		if err := h.eventRepo.MarkDone(ctx, env.EventID, h.consumerID, []byte(`{"status":"ok"}`)); err != nil {
			if errors.Is(err, repo.ErrClaimLost) {
				h.log.Warn("mark done: claim lost", slog.String("event_id", env.EventID.String()))
			} else {
				h.log.Error("mark done failed", slog.Any("error", err), slog.String("event_id", env.EventID.String()))
			}
			return Result{Committable: false, EventID: env.EventID, Error: err}
		}
		return Result{Committable: true, EventID: env.EventID}
	}

	classified := ClassifyError(cmdErr)
	if IsPermanent(classified) {
		reason := cmdErr.Error()
		if err := h.eventRepo.MarkDLQ(ctx, env.EventID, h.consumerID, reason); err != nil {
			if errors.Is(err, repo.ErrClaimLost) {
				h.log.Warn("mark dlq: claim lost", slog.String("event_id", env.EventID.String()))
			} else {
				h.log.Error("mark dlq failed", slog.Any("error", err), slog.String("event_id", env.EventID.String()))
			}
			return Result{Committable: false, EventID: env.EventID, Error: err}
		}
		h.sendDLQ(ctx, raw, env.EventID, reason)
		return Result{Committable: true, EventID: env.EventID, Error: cmdErr}
	}

	return Result{Committable: false, EventID: env.EventID, Error: cmdErr}
}

func (h *Handler) handleOnce(ctx context.Context, env *Envelope) error {
	switch env.EventType {
	case "media.attach":
		return h.handleAttach(ctx, env)
	case "media.detach":
		return h.handleDetach(ctx, env)
	default:
		return PermanentError{fmt.Errorf("unknown event type: %s", env.EventType)}
	}
}

func (h *Handler) handleAttach(ctx context.Context, env *Envelope) error {
	payload, err := DecodeAttach(env.Payload)
	if err != nil {
		return err
	}
	if err := h.mediaSvc.AttachMedia(ctx, payload.MediaID, payload.OwnerID); err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			return RetryableError{err}
		}
		return ClassifyError(err)
	}
	return nil
}

func (h *Handler) handleDetach(ctx context.Context, env *Envelope) error {
	payload, err := DecodeDetach(env.Payload)
	if err != nil {
		return err
	}
	if err := h.mediaSvc.DeleteMedia(ctx, payload.OwnerID, payload.MediaID); err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			return nil
		}
		return ClassifyError(err)
	}
	return nil
}

func (h *Handler) sendDLQ(ctx context.Context, raw []byte, eventID uuid.UUID, reason string) {
	if h.dlq == nil {
		return
	}
	if err := h.dlq.Publish(ctx, raw, eventID, reason); err != nil {
		h.log.Error("dlq publish failed", slog.Any("error", err), slog.String("event_id", eventID.String()))
	}
}

func (h *Handler) fingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
