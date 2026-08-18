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
	oldRoot := outputRoot
	outputRoot = t.TempDir()
	defer func() { outputRoot = oldRoot }()

	ctx := context.Background()
	err := GenerateThumbnail(ctx, "testdata/video.mp4", "thumb.jpg", KindVideo, 0)
	require.NoError(t, err)

	stat, err := os.Stat(filepath.Join(outputRoot, "thumb.jpg"))
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailAudio(t *testing.T) {
	oldRoot := outputRoot
	outputRoot = t.TempDir()
	defer func() { outputRoot = oldRoot }()

	ctx := context.Background()
	err := GenerateThumbnail(ctx, "testdata/audio.mp3", "waveform.png", KindAudio, 0)
	require.NoError(t, err)

	stat, err := os.Stat(filepath.Join(outputRoot, "waveform.png"))
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailImage(t *testing.T) {
	oldRoot := outputRoot
	outputRoot = t.TempDir()
	defer func() { outputRoot = oldRoot }()

	ctx := context.Background()
	err := GenerateThumbnail(ctx, "testdata/image.png", "thumb.jpg", KindImage, 0)
	require.NoError(t, err)

	stat, err := os.Stat(filepath.Join(outputRoot, "thumb.jpg"))
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestTranscodeVideo(t *testing.T) {
	oldRoot := outputRoot
	outputRoot = t.TempDir()
	defer func() { outputRoot = oldRoot }()

	ctx := context.Background()
	_, err := Transcode(ctx, "testdata/video.mp4", "output.mp4", KindVideo)
	require.NoError(t, err)

	stat, err := os.Stat(filepath.Join(outputRoot, "output.mp4"))
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailCancel(t *testing.T) {
	oldRoot := outputRoot
	outputRoot = t.TempDir()
	defer func() { outputRoot = oldRoot }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := GenerateThumbnail(ctx, "testdata/video.mp4", "thumb.jpg", KindVideo, 0)
	require.Error(t, err)
}

func TestTranscodeCancel(t *testing.T) {
	oldRoot := outputRoot
	outputRoot = t.TempDir()
	defer func() { outputRoot = oldRoot }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Transcode(ctx, "testdata/video.mp4", "out.mp4", KindVideo)
	require.Error(t, err)
}
