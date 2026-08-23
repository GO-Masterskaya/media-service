package events

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mediaservice/internal/repo"
)

type stubMediaSvc struct {
	media     *repo.Media
	attachErr error
	deleteErr error
}

func (s *stubMediaSvc) GetMedia(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	if s.media == nil {
		return nil, status.Error(codes.NotFound, "media not found")
	}
	return s.media, nil
}

func (s *stubMediaSvc) AttachMedia(ctx context.Context, mediaID uuid.UUID, ownerID uuid.UUID) error {
	return s.attachErr
}

func (s *stubMediaSvc) DeleteMedia(ctx context.Context, callerID, mediaID uuid.UUID) error {
	return s.deleteErr
}

type stubEventRepo struct {
	claimEvent  *repo.ProcessedEvent
	claimed     bool
	claimErr    error
	markDoneErr error
	markDLQErr  error
}

func (s *stubEventRepo) Claim(ctx context.Context, eventID uuid.UUID, fingerprint, owner string, lease time.Duration) (*repo.ProcessedEvent, bool, error) {
	if s.claimErr != nil {
		return nil, false, s.claimErr
	}
	return s.claimEvent, s.claimed, nil
}

func (s *stubEventRepo) MarkDone(ctx context.Context, eventID uuid.UUID, owner string, result []byte) error {
	return s.markDoneErr
}

func (s *stubEventRepo) MarkDLQ(ctx context.Context, eventID uuid.UUID, owner, reason string) error {
	return s.markDLQErr
}

func (s *stubEventRepo) DeleteTerminalOlderThan(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	return 0, nil
}

type stubDLQ struct {
	published bool
	err       error
}

func (s *stubDLQ) Publish(ctx context.Context, original []byte, eventID uuid.UUID, reason string) error {
	s.published = true
	return s.err
}

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func makeEnvelope(t *testing.T, eventType string, payload string) []byte {
	t.Helper()
	return []byte(`{
		"event_id":"11111111-1111-1111-1111-111111111111",
		"event_type":"` + eventType + `",
		"timestamp":"2026-01-01T00:00:00Z",
		"payload":` + payload + `
	}`)
}

func TestHandle_InvalidJSON_DLQ(t *testing.T) {
	dlq := &stubDLQ{}
	h := NewHandler(nil, &stubEventRepo{}, dlq, "test-consumer", testLog())
	res := h.Handle(context.Background(), []byte("not json"))
	assert.True(t, res.Committable)
	assert.Error(t, res.Error)
	assert.True(t, dlq.published)
}

func TestHandle_AlreadyProcessed(t *testing.T) {
	h := NewHandler(nil, &stubEventRepo{claimed: false}, &stubDLQ{}, "test-consumer", testLog())
	raw := makeEnvelope(t, "media.attach", `{}`)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.NoError(t, res.Error)
}

func TestHandle_ClaimHeld_NotCommittable(t *testing.T) {
	repoStub := &stubEventRepo{claimErr: repo.ErrClaimHeld}
	h := NewHandler(nil, repoStub, &stubDLQ{}, "test-consumer", testLog())
	raw := makeEnvelope(t, "media.attach", `{}`)
	res := h.Handle(context.Background(), raw)
	assert.False(t, res.Committable)
	assert.ErrorIs(t, res.Error, repo.ErrClaimHeld)
}

func TestHandle_FingerprintConflict_DLQ(t *testing.T) {
	repoStub := &stubEventRepo{claimErr: repo.ErrFingerprintConflict}
	dlq := &stubDLQ{}
	h := NewHandler(nil, repoStub, dlq, "test-consumer", testLog())
	raw := makeEnvelope(t, "media.attach", `{}`)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.ErrorIs(t, res.Error, repo.ErrFingerprintConflict)
	assert.True(t, dlq.published)
}

func TestHandle_Attach_Success(t *testing.T) {
	svc := &stubMediaSvc{}
	repoStub := &stubEventRepo{claimed: true}
	h := NewHandler(svc, repoStub, &stubDLQ{}, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.attach", payload)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.NoError(t, res.Error)
}

func TestHandle_Attach_OwnerMismatch_Permanent_DLQ(t *testing.T) {
	svc := &stubMediaSvc{attachErr: status.Error(codes.PermissionDenied, "owner mismatch")}
	repoStub := &stubEventRepo{claimed: true}
	dlq := &stubDLQ{}
	h := NewHandler(svc, repoStub, dlq, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}`
	raw := makeEnvelope(t, "media.attach", payload)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.True(t, dlq.published)
}

func TestHandle_Attach_MediaNotFound_Retryable(t *testing.T) {
	svc := &stubMediaSvc{attachErr: status.Error(codes.NotFound, "media not found")}
	repoStub := &stubEventRepo{claimed: true}
	dlq := &stubDLQ{}
	h := NewHandler(svc, repoStub, dlq, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.attach", payload)
	res := h.Handle(context.Background(), raw)
	assert.False(t, res.Committable)
	assert.False(t, dlq.published)
	assert.True(t, IsRetryable(res.Error))
}

func TestHandle_Attach_MarkDoneFailure_NotCommittable(t *testing.T) {
	svc := &stubMediaSvc{}
	repoStub := &stubEventRepo{claimed: true, markDoneErr: errors.New("db down")}
	h := NewHandler(svc, repoStub, &stubDLQ{}, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.attach", payload)
	res := h.Handle(context.Background(), raw)
	assert.False(t, res.Committable)
	assert.Error(t, res.Error)
}

func TestHandle_Detach_Success(t *testing.T) {
	svc := &stubMediaSvc{}
	repoStub := &stubEventRepo{claimed: true}
	h := NewHandler(svc, repoStub, &stubDLQ{}, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.detach", payload)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.NoError(t, res.Error)
}

func TestHandle_Detach_NotFound_Idempotent(t *testing.T) {
	svc := &stubMediaSvc{deleteErr: status.Error(codes.NotFound, "media not found")}
	repoStub := &stubEventRepo{claimed: true}
	h := NewHandler(svc, repoStub, &stubDLQ{}, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.detach", payload)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.NoError(t, res.Error)
}

func TestHandle_Detach_Retryable_NotCommittable(t *testing.T) {
	svc := &stubMediaSvc{deleteErr: errors.New("transient db error")}
	repoStub := &stubEventRepo{claimed: true}
	dlq := &stubDLQ{}
	h := NewHandler(svc, repoStub, dlq, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.detach", payload)
	res := h.Handle(context.Background(), raw)
	assert.False(t, res.Committable)
	assert.False(t, dlq.published)
	assert.True(t, IsRetryable(res.Error))
}

func TestHandle_Detach_Permanent_DLQ(t *testing.T) {
	svc := &stubMediaSvc{deleteErr: status.Error(codes.PermissionDenied, "access denied")}
	repoStub := &stubEventRepo{claimed: true}
	dlq := &stubDLQ{}
	h := NewHandler(svc, repoStub, dlq, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.detach", payload)
	res := h.Handle(context.Background(), raw)
	assert.True(t, res.Committable)
	assert.True(t, dlq.published)
}

func TestHandle_Detach_MarkDLQFailure_NotCommittable(t *testing.T) {
	svc := &stubMediaSvc{deleteErr: status.Error(codes.PermissionDenied, "access denied")}
	repoStub := &stubEventRepo{claimed: true, markDLQErr: errors.New("db down")}
	h := NewHandler(svc, repoStub, &stubDLQ{}, "test-consumer", testLog())
	payload := `{"media_id":"22222222-2222-2222-2222-222222222222","owner_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
	raw := makeEnvelope(t, "media.detach", payload)
	res := h.Handle(context.Background(), raw)
	assert.False(t, res.Committable)
	assert.Error(t, res.Error)
}
