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
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "thumb.jpg")

	ctx := context.Background()
	_, err := GenerateThumbnail(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailAudio(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "waveform.png")

	ctx := context.Background()
	_, err := GenerateThumbnail(ctx, outDir, "testdata/audio.mp3", outputPath, KindAudio, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailImage(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "thumb.jpg")

	ctx := context.Background()
	_, err := GenerateThumbnail(ctx, outDir, "testdata/image.png", outputPath, KindImage, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestTranscodeVideo(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "output.mp4")

	ctx := context.Background()
	_, err := Transcode(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailCancel(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "thumb.jpg")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := GenerateThumbnail(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo, 0)
	require.Error(t, err)
}

func TestTranscodeCancel(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "out.mp4")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Transcode(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo)
	require.Error(t, err)
}

func TestResolveSafePath(t *testing.T) {
	tmpDir := t.TempDir()

	test := []struct {
		name       string
		outputRoot string
		outputPath string
		wantErr    bool
	}{
		{
			name:       "valid relative path",
			outputRoot: tmpDir,
			outputPath: "output.mp4",
			wantErr:    false,
		},
		{
			name:       "valid relative path with ..data prefix",
			outputRoot: tmpDir,
			outputPath: "..data/output.mp4",
			wantErr:    false,
		},
		{
			name:       "valid path inside subfolder",
			outputRoot: tmpDir,
			outputPath: "sub/dir/output.mp4",
			wantErr:    false,
		},
		{
			name:       "path travelsal with ../..",
			outputRoot: tmpDir,
			outputPath: "../../etc/passswd",
			wantErr:    true,
		},
		{
			name:       "absolute path outside root",
			outputRoot: tmpDir,
			outputPath: "/etc/passwd",
			wantErr:    true,
		},
		{
			name:       "absolute path inside root",
			outputRoot: "/tmp/processing",
			outputPath: "/tmp/processing/sub/dir/out.mp4",
			wantErr:    false,
		},
		{
			name:       "root is symlink",
			outputRoot: "/var/tmp",
			outputPath: "/var/tmp/out.mp4",
			wantErr:    false,
		},
	}

	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveSafePath(tt.outputRoot, tt.outputPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveSavePath() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
