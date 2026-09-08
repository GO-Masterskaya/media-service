package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
)

var (
	handlerMetricsOnce sync.Once
	eventsProcessed    *prometheus.CounterVec
	eventsDLQ          *prometheus.CounterVec
	eventsRetried      prometheus.Counter
	eventsFailed       *prometheus.CounterVec
)

func initHandlerMetrics() {
	handlerMetricsOnce.Do(func() {
		eventsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "handler_events_processed_total",
			Help: "Total events processed by type and outcome",
		}, []string{"event_type", "outcome"})
		eventsDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "handler_dlq_total",
			Help: "Total events sent to DLQ",
		}, []string{"reason"})
		eventsRetried = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "handler_events_retried_total",
			Help: "Total retryable events",
		})
		eventsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "handler_events_failed_total",
			Help: "Total unrecoverable failures",
		}, []string{"phase"})
		prometheus.MustRegister(eventsProcessed, eventsDLQ, eventsRetried, eventsFailed)
	})
}

type DLQPublisher interface {
	Publish(ctx context.Context, original []byte, eventID uuid.UUID, reason string) error
	Close() error
}

type HandlerConfig struct {
	LeaseDuration time.Duration
	MaxAttempts   int
}

type Handler struct {
	mediaSvc      MediaService
	eventRepo     repo.ProcessedEventRepo
	dlq           DLQPublisher
	consumerID    string
	leaseDuration time.Duration
	maxAttempts   int
	log           *slog.Logger

	processedCounter *prometheus.CounterVec
	dlqCounter       *prometheus.CounterVec
	retryCounter     prometheus.Counter
	failCounter      *prometheus.CounterVec
}

func NewHandler(
	mediaSvc MediaService,
	eventRepo repo.ProcessedEventRepo,
	dlq DLQPublisher,
	consumerID string,
	log *slog.Logger,
) (*Handler, error) {
	return NewHandlerWithConfig(mediaSvc, eventRepo, dlq, consumerID, HandlerConfig{
		LeaseDuration: 30 * time.Second,
		MaxAttempts:   3,
	}, log)
}

func NewHandlerWithConfig(
	mediaSvc MediaService,
	eventRepo repo.ProcessedEventRepo,
	dlq DLQPublisher,
	consumerID string,
	cfg HandlerConfig,
	log *slog.Logger,
) (*Handler, error) {
	if log == nil {
		log = slog.Default()
	}
	if consumerID == "" {
		return nil, fmt.Errorf("consumerID required")
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	initHandlerMetrics()

	return &Handler{
		mediaSvc:         mediaSvc,
		eventRepo:        eventRepo,
		dlq:              dlq,
		consumerID:       consumerID,
		leaseDuration:    cfg.LeaseDuration,
		maxAttempts:      cfg.MaxAttempts,
		log:              log,
		processedCounter: eventsProcessed,
		dlqCounter:       eventsDLQ,
		retryCounter:     eventsRetried,
		failCounter:      eventsFailed,
	}, nil
}

type Result struct {
	Committable bool
	EventID     uuid.UUID
	Error       error
}

func (h *Handler) Handle(ctx context.Context, raw []byte) Result {
	env, err := DecodeEnvelope(raw)
	if err != nil {
		h.failCounter.WithLabelValues("decode").Inc()
		dlqID := h.deterministicID(raw)
		if pubErr := h.sendDLQ(ctx, raw, dlqID, "invalid envelope: "+err.Error()); pubErr != nil {
			return Result{Committable: false, Error: pubErr}
		}
		return Result{Committable: true, Error: err}
	}

	fingerprint := h.fingerprint(raw)
	_, claimed, err := h.eventRepo.Claim(ctx, env.EventID, fingerprint, h.consumerID, h.leaseDuration)
	if err != nil {
		switch {
		case errors.Is(err, repo.ErrFingerprintConflict):
			h.failCounter.WithLabelValues("fingerprint_conflict").Inc()

			if pubErr := h.sendDLQ(ctx, raw, env.EventID, "fingerprint conflict"); pubErr != nil {
				return Result{Committable: false, EventID: env.EventID, Error: pubErr}
			}
			if markErr := h.eventRepo.MarkDLQ(ctx, env.EventID, h.consumerID, "fingerprint conflict"); markErr != nil {
				h.log.Error("mark dlq after fingerprint conflict failed", slog.Any("error", markErr), slog.String("event_id", env.EventID.String()))
				return Result{Committable: false, EventID: env.EventID, Error: markErr}
			}
			h.dlqCounter.WithLabelValues("fingerprint_conflict").Inc()
			return Result{Committable: true, EventID: env.EventID, Error: err}

		case errors.Is(err, repo.ErrClaimHeld):
			// Другой инстанс держит живой claim. Это не ошибка обработки,
			// а состояние очереди. Счётчик retry_count в processed_events
			// принадлежит тому, кто владеет claim'ом. Мы просто ждём.
			return Result{Committable: false, EventID: env.EventID, Error: RetryableError{err}}
		default:
			h.log.Error("claim failed", slog.Any("error", err), slog.String("event_id", env.EventID.String()))
			h.failCounter.WithLabelValues("claim").Inc()
			return Result{Committable: false, EventID: env.EventID, Error: RetryableError{err}}
		}
	}
	if !claimed {
		h.processedCounter.WithLabelValues(env.EventType, "skipped").Inc()
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
				h.failCounter.WithLabelValues("mark_done").Inc()
			}
			return Result{Committable: false, EventID: env.EventID, Error: err}
		}
		h.processedCounter.WithLabelValues(env.EventType, "success").Inc()
		return Result{Committable: true, EventID: env.EventID}
	}

	classified := ClassifyError(cmdErr)
	if IsRetryable(classified) {
		h.retryCounter.Inc()
		attempts, bumpErr := h.eventRepo.BumpAttempt(ctx, env.EventID, h.consumerID)
		if bumpErr != nil {
			if errors.Is(bumpErr, repo.ErrClaimLost) {
				h.log.Warn("bump attempt: claim lost", slog.String("event_id", env.EventID.String()))
			} else {
				h.log.Error("bump attempt failed", slog.Any("error", bumpErr), slog.String("event_id", env.EventID.String()))
				h.failCounter.WithLabelValues("bump_attempt").Inc()
			}
			return Result{Committable: false, EventID: env.EventID, Error: bumpErr}
		}
		if attempts >= h.maxAttempts {
			reason := fmt.Sprintf("max attempts (%d) exceeded: %v", h.maxAttempts, cmdErr)
			if pubErr := h.sendDLQ(ctx, raw, env.EventID, reason); pubErr != nil {
				return Result{Committable: false, EventID: env.EventID, Error: pubErr}
			}
			if err := h.eventRepo.MarkDLQ(ctx, env.EventID, h.consumerID, reason); err != nil {
				if errors.Is(err, repo.ErrClaimLost) {
					h.log.Warn("mark dlq: claim lost", slog.String("event_id", env.EventID.String()))
				} else {
					h.log.Error("mark dlq failed", slog.Any("error", err), slog.String("event_id", env.EventID.String()))
					h.failCounter.WithLabelValues("mark_dlq").Inc()
				}
				return Result{Committable: false, EventID: env.EventID, Error: err}
			}
			h.dlqCounter.WithLabelValues("max_attempts").Inc()
			return Result{Committable: true, EventID: env.EventID, Error: cmdErr}
		}
		return Result{Committable: false, EventID: env.EventID, Error: cmdErr}
	}

	if IsPermanent(classified) {
		reason := cmdErr.Error()
		if pubErr := h.sendDLQ(ctx, raw, env.EventID, reason); pubErr != nil {
			return Result{Committable: false, EventID: env.EventID, Error: pubErr}
		}
		if err := h.eventRepo.MarkDLQ(ctx, env.EventID, h.consumerID, reason); err != nil {
			if errors.Is(err, repo.ErrClaimLost) {
				h.log.Warn("mark dlq: claim lost", slog.String("event_id", env.EventID.String()))
			} else {
				h.log.Error("mark dlq failed", slog.Any("error", err), slog.String("event_id", env.EventID.String()))
				h.failCounter.WithLabelValues("mark_dlq").Inc()
			}
			return Result{Committable: false, EventID: env.EventID, Error: err}
		}
		h.dlqCounter.WithLabelValues("permanent").Inc()
		return Result{Committable: true, EventID: env.EventID, Error: cmdErr}
	}

	h.failCounter.WithLabelValues("unclassified").Inc()
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

func (h *Handler) sendDLQ(ctx context.Context, raw []byte, eventID uuid.UUID, reason string) error {
	if h.dlq == nil {
		return fmt.Errorf("dlq publisher not configured")
	}
	if err := h.dlq.Publish(ctx, raw, eventID, reason); err != nil {
		h.log.Error("dlq publish failed", slog.Any("error", err), slog.String("event_id", eventID.String()))
		return fmt.Errorf("dlq publish: %w", err)
	}
	return nil
}

func (h *Handler) fingerprint(raw []byte) string {
	var v map[string]json.RawMessage
	if err := json.Unmarshal(raw, &v); err != nil {
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	canonical, err := json.Marshal(v)
	if err != nil {
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// deterministicID возвращает детерминированный UUIDv5 из raw для poison messages,
// чтобы не сбрасывать все битые envelope в одну партицию Kafka.
func (h *Handler) deterministicID(raw []byte) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, raw)
}
