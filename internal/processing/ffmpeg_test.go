package processing

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateThumbnailVideo(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "thumb.jpg")

	err := GenerateThumbnail(ctx, "testdata/video.mp4", outputPath, KindVideo, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailAudio(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "waveform.png")

	err := GenerateThumbnail(ctx, "testdata/audio.mp3", outputPath, KindAudio, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailImage(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "thumb.jpg")

	err := GenerateThumbnail(ctx, "testdata/image.png", outputPath, KindImage, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestTranscodeVideo(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.mp4")

	err := Transcode(ctx, "testdata/video.mp4", outputPath, KindVideo)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "thumb.jpg")

	err := GenerateThumbnail(ctx, "testdata/video.mp4", outputPath, KindVideo, 0)
	require.Error(t, err)
}

func TestTranscodeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "out.mp4")

	err := Transcode(ctx, "testdata/video.mp4", outputPath, KindVideo)
	require.Error(t, err)
}
