package acceptance_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/stretchr/testify/require"

	mediav1 "mediaservice/proto/media/v1"
)

func (s *AcceptanceSuite) TestVideo_ProcessingToReady() {
	t := s.T()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}

	videoPath := filepath.Join(s.tempDir, "sample.mp4")
	cmd := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "color=c=blue:s=320x240:d=2",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", "-t", "2",
		videoPath,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "ffmpeg: %s", out)

	body, err := os.ReadFile(videoPath)
	require.NoError(t, err)

	up := s.upload(t, uploadOpts{
		filename:      "sample.mp4",
		mime:          "video/mp4",
		body:          body,
		makeThumbnail: true,
		transcode:     true,
	})
	require.Equal(t, mediav1.MediaStatus_PROCESSING, up.Status)

	m := s.waitStatus(t, up.MediaId, mediav1.MediaStatus_READY)
	require.NotNil(t, m.Metadata)

	for _, variant := range []string{"thumb", "r_720"} {
		urlResp, err := s.client.GetDownloadURL(s.authCtx(), &mediav1.GetDownloadURLRequest{
			MediaId: up.MediaId,
			Variant: variant,
		})
		require.NoError(t, err, "variant %s", variant)
		require.NotEmpty(t, urlResp.Url)
		got := httpGetBytes(t, urlResp.Url)
		require.NotEmpty(t, got, "empty body for %s", variant)
	}
}

func (s *AcceptanceSuite) TestTTL_ReaperDeletesExpiredMedia() {
	t := s.T()

	up := s.upload(t, uploadOpts{
		filename: "ttl.png",
		mime:     "image/png",
		body:     png64,
		ttl:      time.Second,
	})
	s.waitStatus(t, up.MediaId, mediav1.MediaStatus_STORED)

	// expires_at = now+1s; reaper тикает каждые 200ms.
	s.waitMediaGone(t, up.MediaId)
}

func (s *AcceptanceSuite) TestDeferred_RateLimitAndMetrics() {
	s.T().Skip("blocked on #21: rate/stream limits and /metrics scrape")
}

func (s *AcceptanceSuite) TestDeferred_EngineConcurrencyAndCrashRecovery() {
	s.T().Skip("engine concurrency/crash-recovery acceptance is a dedicated follow-up")
}
