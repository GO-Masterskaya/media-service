package acceptance_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
	mediav1 "mediaservice/proto/media/v1"
)

func (s *AcceptanceSuite) TestAudio_UploadStored() {
	t := s.T()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}

	audioPath := filepath.Join(s.tempDir, "sample.mp3")
	cmd := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "sine=f=440:d=2",
		"-c:a", "libmp3lame", "-t", "2",
		audioPath,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "ffmpeg: %s", out)

	body, err := os.ReadFile(audioPath)
	require.NoError(t, err)

	up := s.upload(t, uploadOpts{
		filename: "sample.mp3",
		mime:     "audio/mpeg",
		body:     body,
	})
	require.Equal(t, mediav1.MediaStatus_STORED, up.Status)

	m := s.waitStatus(t, up.MediaId, mediav1.MediaStatus_STORED)
	require.NotNil(t, m.Metadata)
	require.Equal(t, mediav1.MediaKind_AUDIO, m.Kind)

	streamed := s.downloadStreamAll(t, up.MediaId, "original")
	require.True(t, bytes.Equal(body, streamed), "audio stream body mismatch")
}

func (s *AcceptanceSuite) TestIdempotency_ConflictDifferentParams() {
	t := s.T()
	key := uuid.NewString()

	first := s.upload(t, uploadOpts{
		filename:       "pixel.png",
		mime:           "image/png",
		body:           png64,
		idempotencyKey: key,
	})
	require.NotEmpty(t, first.MediaId)

	err := s.uploadExpectError(t, uploadOpts{
		filename:       "pixel.png",
		mime:           "image/png",
		body:           png64,
		idempotencyKey: key,
		makeThumbnail:  true, // params fingerprint mismatch
	})
	requireGRPCCode(t, err, codes.AlreadyExists)
}

func (s *AcceptanceSuite) TestHealth_LivezReadyz() {
	t := s.T()
	require.NotEmpty(t, s.httpBaseURL)

	livez := s.httpGetStatus(t, s.httpBaseURL+"/livez")
	require.Equal(t, http.StatusOK, livez)

	readyz := s.httpGetStatus(t, s.httpBaseURL+"/readyz")
	require.Equal(t, http.StatusOK, readyz)

	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	t.Cleanup(func() {
		s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	})

	notReady := s.httpGetStatus(t, s.httpBaseURL+"/readyz")
	require.Equal(t, http.StatusServiceUnavailable, notReady)
	stillLive := s.httpGetStatus(t, s.httpBaseURL+"/livez")
	require.Equal(t, http.StatusOK, stillLive)
}

func (s *AcceptanceSuite) TestQuota_ExceededReturnsResourceExhausted() {
	t := s.T()

	client := s.newQuotaLimitedClient(t, 1) // 1 byte — любой PNG не влезет
	stream, err := client.Upload(s.authCtx())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Init{Init: &mediav1.UploadInit{
			OwnerId:        s.ownerID.String(),
			Filename:       "quota.png",
			Mime:           "image/png",
			ExpectedSize:   uint64(len(png64)),
			IdempotencyKey: uuid.NewString(),
		}},
	}))
	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Chunk{Chunk: png64},
	}))
	_, err = stream.CloseAndRecv()
	requireGRPCCode(t, err, codes.ResourceExhausted)

	// Обычный клиент (без квоты) всё ещё работает.
	up := s.upload(t, uploadOpts{filename: "after-quota.png", body: png64})
	require.NotEmpty(t, up.MediaId)
}

func (s *AcceptanceSuite) TestProcessing_CorruptSourceGoesFailed() {
	t := s.T()

	mediaID := uuid.New()
	key := fmt.Sprintf("%s/%s/original.bin", s.ownerID.String(), mediaID.String())
	garbage := []byte("not-a-valid-media-file-for-ffmpeg")
	require.NoError(t, s.sto.PutObject(s.ctx, key, bytes.NewReader(garbage), int64(len(garbage)), "video/mp4"))

	_, err := s.mediaRepo.InsertWithJobs(s.ctx, repo.Media{
		ID:                mediaID,
		OwnerID:           s.ownerID,
		Kind:              repo.MediaKindVideo,
		OrigFilename:      "broken.mp4",
		Mime:              "video/mp4",
		SizeBytes:         int64(len(garbage)),
		StorageKey:        key,
		IdempotencyKey:    uuid.NewString(),
		BodyFingerprint:   uuid.NewString(),
		ParamsFingerprint: uuid.NewString(),
	}, []string{"thumbnail"})
	require.NoError(t, err)

	failed := s.waitFailed(t, mediaID.String())
	require.NotEmpty(t, failed.Error)

	// Сервис жив после FAILED.
	up := s.upload(t, uploadOpts{filename: "alive-after-fail.png", body: png64})
	require.NotEmpty(t, up.MediaId)
}

func (s *AcceptanceSuite) TestEngine_CrashRecoveryRequeuesStaleLease() {
	t := s.T()

	mediaID := uuid.New()
	key := fmt.Sprintf("%s/%s/original.png", s.ownerID.String(), mediaID.String())
	require.NoError(t, s.sto.PutObject(s.ctx, key, bytes.NewReader(png64), int64(len(png64)), "image/png"))

	_, err := s.mediaRepo.InsertWithJobs(s.ctx, repo.Media{
		ID:                mediaID,
		OwnerID:           s.ownerID,
		Kind:              repo.MediaKindImage,
		OrigFilename:      "stale.png",
		Mime:              "image/png",
		SizeBytes:         int64(len(png64)),
		StorageKey:        key,
		IdempotencyKey:    uuid.NewString(),
		BodyFingerprint:   uuid.NewString(),
		ParamsFingerprint: uuid.NewString(),
	}, nil)
	require.NoError(t, err)

	jobID := uuid.New()
	_, err = s.pool.Exec(s.ctx, `
		INSERT INTO processing_jobs (id, media_id, type, status, locked_by, lease_until, attempts)
		VALUES ($1, $2, 'thumbnail', 'running', 'dead-worker', now() - interval '30 seconds', 0)
	`, jobID, mediaID)
	require.NoError(t, err)

	_, err = s.pool.Exec(s.ctx, `UPDATE media SET status = 'processing' WHERE id = $1`, mediaID)
	require.NoError(t, err)

	reaped, err := s.jobRepo.ReapExpiredLeases(s.ctx, 3, repo.DefaultJobBackoff(), 100)
	require.NoError(t, err)
	require.GreaterOrEqual(t, reaped, int64(1))

	var status string
	var attempts int
	err = s.pool.QueryRow(s.ctx,
		`SELECT status, attempts FROM processing_jobs WHERE id = $1`, jobID,
	).Scan(&status, &attempts)
	require.NoError(t, err)
	// Engine мог уже подхватить queued job — допускаем queued или done.
	require.Contains(t, []string{"queued", "running", "done"}, status)
	if status == "queued" {
		require.Equal(t, 1, attempts)
	}

	s.waitStatus(t, mediaID.String(), mediav1.MediaStatus_READY)
}

func (s *AcceptanceSuite) TestEngine_InFlightWorkersRespectConcurrency() {
	t := s.T()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}

	videoPath := filepath.Join(s.tempDir, "concurrency.mp4")
	cmd := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "color=c=red:s=320x240:d=1",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", "-t", "1",
		videoPath,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "ffmpeg: %s", out)
	body, err := os.ReadFile(videoPath)
	require.NoError(t, err)

	const n = 4
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		up := s.upload(t, uploadOpts{
			filename:      fmt.Sprintf("c-%d.mp4", i),
			mime:          "video/mp4",
			body:          body,
			makeThumbnail: true,
			transcode:     true,
		})
		ids = append(ids, up.MediaId)
	}

	deadline := time.Now().Add(90 * time.Second)
	maxInFlight := 0.0
	for time.Now().Before(deadline) {
		bodyMetrics := s.scrapeMetrics(t)
		if v, ok := parsePrometheusGauge(bodyMetrics, "media_processing_in_flight_workers"); ok {
			if v > maxInFlight {
				maxInFlight = v
			}
			require.LessOrEqual(t, v, 2.0, "WORKER_CONCURRENCY=2 must cap in-flight workers")
		}

		allReady := true
		for _, id := range ids {
			m, err := s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{MediaId: id})
			require.NoError(t, err)
			if m.Status == mediav1.MediaStatus_FAILED {
				t.Fatalf("media %s failed: %v", id, m.Error)
			}
			if m.Status != mediav1.MediaStatus_READY {
				allReady = false
			}
		}
		if allReady {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, id := range ids {
		s.waitStatus(t, id, mediav1.MediaStatus_READY)
	}
	require.Greater(t, maxInFlight, 0.0, "expected to observe in-flight workers while processing")
}

func (s *AcceptanceSuite) TestReconciler_RemovesOrphanObject() {
	t := s.T()

	orphanMediaID := uuid.New()
	key := fmt.Sprintf("%s/%s/original.png", s.ownerID.String(), orphanMediaID.String())
	require.NoError(t, s.sto.PutObject(s.ctx, key, bytes.NewReader(png64), int64(len(png64)), "image/png"))

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		exists, err := storageObjectExists(s.ctx, s.sto, key)
		require.NoError(t, err)
		if !exists {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("orphan object %s was not removed by reconciler", key)
}

func (s *AcceptanceSuite) TestReconciler_CleansStuckDeleting() {
	t := s.T()

	up := s.upload(t, uploadOpts{filename: "stuck-del.png", body: png64})
	mediaID, err := uuid.Parse(up.MediaId)
	require.NoError(t, err)

	_, err = s.pool.Exec(s.ctx, `
		UPDATE media SET status = 'deleting', updated_at = now() - interval '10 seconds'
		WHERE id = $1
	`, mediaID)
	require.NoError(t, err)

	s.waitMediaGone(t, up.MediaId)
}

func (s *AcceptanceSuite) httpGetStatus(t *testing.T, url string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func parsePrometheusGauge(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		metric := fields[0]
		if metric != name && !strings.HasPrefix(metric, name+"{") {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%f", &v); err == nil {
			return v, true
		}
	}
	return 0, false
}

func storageObjectExists(ctx context.Context, sto storage.Interface, key string) (bool, error) {
	found := false
	err := sto.ForEachObject(ctx, key, func(obj storage.ObjectInfo) error {
		if obj.Key == key {
			found = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if found {
		return true, nil
	}
	// Fallback: try GetObject read.
	rc, err := sto.GetObject(ctx, key)
	if err != nil {
		return false, nil
	}
	defer func() { _ = rc.Close() }()
	_, err = io.ReadAll(rc)
	return err == nil, nil
}
