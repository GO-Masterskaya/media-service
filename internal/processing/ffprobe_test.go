package processing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProbeImage(t *testing.T) {
	ctx := context.Background()
	info, err := Probe(ctx, "testdata/image.png")

	require.NoError(t, err)
	assert.Equal(t, KindImage, info.Kind)
	assert.Equal(t, 10, info.Width)
	assert.Equal(t, 10, info.Height)
	assert.NotEmpty(t, info.Codec)
}

func TestProbeVideo(t *testing.T) {
	ctx := context.Background()
	info, err := Probe(ctx, "testdata/video.mp4")

	require.NoError(t, err)
	assert.Equal(t, KindVideo, info.Kind)
	assert.Equal(t, 320, info.Width)
	assert.Equal(t, 240, info.Height)
	assert.NotEmpty(t, info.Codec)
	assert.Greater(t, info.Duration, time.Duration(0))
}

func TestProbeAudio(t *testing.T) {
	ctx := context.Background()
	info, err := Probe(ctx, "testdata/audio.mp3")

	require.NoError(t, err)
	assert.Equal(t, KindAudio, info.Kind)
	assert.Greater(t, info.Duration, time.Duration(0))
	assert.NotEmpty(t, info.Codec)
}

func TestProbeFakeExtension(t *testing.T) {
	ctx := context.Background()
	info, err := Probe(ctx, "testdata/fake.png")

	require.NoError(t, err)
	assert.Equal(t, KindImage, info.Kind)
	assert.Contains(t, info.FormatName, "jpeg")
}

func TestProbeCorrupt(t *testing.T) {
	ctx := context.Background()
	_, err := Probe(ctx, "testdata/corrupt.bin")

	require.Error(t, err)
}

func TestProbeMuteVideo(t *testing.T) {
	ctx := context.Background()
	info, err := Probe(ctx, "testdata/mute_video.mp4")
	require.NoError(t, err)
	assert.Equal(t, KindVideo, info.Kind)
}

func TestProbeAnimatedGIF(t *testing.T) {
	ctx := context.Background()
	info, err := Probe(ctx, "testdata/animated.gif")
	require.NoError(t, err)
	assert.Equal(t, KindVideo, info.Kind)
}
func TestProbeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Probe(ctx, "testdata/video.mp4")
	require.Error(t, err)
}
