package acceptance_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"

	mediav1 "github.com/GO-Masterskaya/media-service/proto/media/v1"
)

func (s *AcceptanceSuite) TestMetrics_ScrapeExposesGRPCSeries() {
	t := s.T()

	_, err := s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{
		MediaId: uuid.NewString(),
	})
	require.Error(t, err) // NotFound — метрика всё равно пишется

	body := s.scrapeMetrics(t)
	require.Contains(t, body, "grpc_requests_total")
	require.Contains(t, body, "grpc_request_duration_seconds")
	require.Contains(t, body, "grpc_active_streams")
	require.Contains(t, body, `method="/media.v1.MediaService/GetMedia"`)
	require.Contains(t, body, `code="NotFound"`)

	// High-cardinality labels запрещены (#21).
	require.NotContains(t, body, `owner_id=`)
	require.NotContains(t, body, `media_id=`)
	require.NotContains(t, body, `token=`)
}

func (s *AcceptanceSuite) TestRateLimit_IsolatesCallers() {
	t := s.T()

	client := s.newLimitedClient(t, limitedClientOpts{
		rps:        0, // только burst, без refill
		burst:      2,
		maxStreams: 8,
		allowlist:  []string{"caller-a", "caller-b"},
	})

	missingID := uuid.NewString()
	for i := 0; i < 2; i++ {
		_, err := client.GetMedia(s.authCtxWithCaller("caller-a"), &mediav1.GetMediaRequest{
			MediaId: missingID,
		})
		require.Error(t, err)
		requireGRPCCode(t, err, codes.NotFound)
	}

	_, err := client.GetMedia(s.authCtxWithCaller("caller-a"), &mediav1.GetMediaRequest{
		MediaId: missingID,
	})
	requireGRPCCode(t, err, codes.ResourceExhausted)

	_, err = client.GetMedia(s.authCtxWithCaller("caller-b"), &mediav1.GetMediaRequest{
		MediaId: missingID,
	})
	require.Error(t, err)
	requireGRPCCode(t, err, codes.NotFound)
}

func (s *AcceptanceSuite) TestStreamLimit_IsolatesCallers() {
	t := s.T()

	client := s.newLimitedClient(t, limitedClientOpts{
		rps:        rate.Limit(1000),
		burst:      1000,
		maxStreams: 1,
		allowlist:  []string{"caller-a", "caller-b"},
	})

	held := s.openHeldUpload(t, client, "caller-a")
	defer func() { _ = held.CloseSend() }()

	// Второй stream того же caller — отказ (ошибка на Send/CloseAndRecv).
	blocked, err := client.Upload(s.authCtxWithCaller("caller-a"))
	require.NoError(t, err)
	err = blocked.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Init{Init: &mediav1.UploadInit{
			OwnerId:        s.ownerID.String(),
			Filename:       "blocked.png",
			Mime:           "image/png",
			ExpectedSize:   uint64(len(png64)),
			IdempotencyKey: uuid.NewString(),
		}},
	})
	if err == nil {
		_, err = blocked.CloseAndRecv()
	}
	requireGRPCCode(t, err, codes.ResourceExhausted)

	// Другой caller не затронут: stream создаётся.
	other, err := client.Upload(s.authCtxWithCaller("caller-b"))
	require.NoError(t, err)
	require.NoError(t, other.CloseSend())
	_, _ = other.CloseAndRecv()
}

func (s *AcceptanceSuite) scrapeMetrics(t *testing.T) string {
	t.Helper()
	require.NotEmpty(t, s.metricsURL)

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, s.metricsURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(raw)
}

// openHeldUpload открывает Upload stream и шлёт только init — stream остаётся
// активным, чтобы держать слот StreamLimiter.
func (s *AcceptanceSuite) openHeldUpload(
	t *testing.T,
	client mediav1.MediaServiceClient,
	callerID string,
) mediav1.MediaService_UploadClient {
	t.Helper()

	stream, err := client.Upload(s.authCtxWithCaller(callerID))
	require.NoError(t, err)

	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Init{Init: &mediav1.UploadInit{
			OwnerId:        s.ownerID.String(),
			Filename:       "held.png",
			Mime:           "image/png",
			ExpectedSize:   uint64(len(png64)),
			IdempotencyKey: uuid.NewString(),
		}},
	}))

	// Дать interceptor’у зафиксировать acquire.
	time.Sleep(50 * time.Millisecond)
	return stream
}
