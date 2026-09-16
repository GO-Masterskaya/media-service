package processing

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateThumbnailVideo(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "thumb.jpg")

	ctx := context.Background()
	_, err := GenerateThumbnail(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo, 0, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailAudio(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "waveform.png")

	ctx := context.Background()
	_, err := GenerateThumbnail(ctx, outDir, "testdata/audio.mp3", outputPath, KindAudio, 0, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestGenerateThumbnailImage(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "thumb.jpg")

	ctx := context.Background()
	_, err := GenerateThumbnail(ctx, outDir, "testdata/image.png", outputPath, KindImage, 0, 0)
	require.NoError(t, err)

	stat, err := os.Stat(outputPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestTranscodeVideo(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "output.mp4")

	ctx := context.Background()
	_, err := Transcode(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo, defaultRendition, 0)
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

	_, err := GenerateThumbnail(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo, 0, 0)
	require.Error(t, err)
}

func TestTranscodeCancel(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "out.mp4")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Transcode(ctx, outDir, "testdata/video.mp4", outputPath, KindVideo, defaultRendition, 0)
	require.Error(t, err)
}

func TestTranscodeUsesConfiguredRendition(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "output.mp4")
	_, err := Transcode(context.Background(), outDir, "testdata/video.mp4", outputPath, KindVideo, 360, 0)
	require.NoError(t, err)

	info, err := Probe(context.Background(), outputPath)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, 360, info.Height)
}

func TestTranscodeDefaultRenditionWhenZero(t *testing.T) {
	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "output.mp4")
	_, err := Transcode(context.Background(), outDir, "testdata/video.mp4", outputPath, KindVideo, 0, 0)
	require.NoError(t, err)

	info, err := Probe(context.Background(), outputPath)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, defaultRendition, info.Height)
}

func TestTranscodeFFMPEGTimeoutBeforeParentDeadline(t *testing.T) {
	// Реальный клип слишком короткий: 50ms часто успевает завершиться.
	// Stub ffmpeg висит дольше родителя, чтобы сработал именно ffmpeg timeout.
	installSlowFFmpegStub(t)

	outDir := t.TempDir()
	outputPath := filepath.Join(outDir, "output.mp4")

	parentTimeout := 5 * time.Second
	ffmpegTimeout := 50 * time.Millisecond
	parentCtx, parentCancel := context.WithTimeout(context.Background(), parentTimeout)
	defer parentCancel()

	started := time.Now()
	_, err := Transcode(parentCtx, outDir, "testdata/video.mp4", outputPath, KindVideo, defaultRendition, ffmpegTimeout)
	elapsed := time.Since(started)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "want deadline in error chain, got: %v", err)
	assert.Less(t, elapsed, parentTimeout/2, "should fail on FFMPEG_TIMEOUT, not parent JOB_TIMEOUT")
	assert.GreaterOrEqual(t, elapsed, ffmpegTimeout)
}

func installSlowFFmpegStub(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "slow.go")
	require.NoError(t, os.WriteFile(src, []byte(`package main

import (
	"os"
	"time"
)

func main() {
	time.Sleep(10 * time.Second)
	os.Exit(1)
}
`), 0o644))

	outName := "ffmpeg"
	if runtime.GOOS == "windows" {
		outName = "ffmpeg.exe"
	}
	out := filepath.Join(dir, outName)
	build := exec.Command("go", "build", "-o", out, src)
	build.Env = os.Environ()
	outBytes, err := build.CombinedOutput()
	require.NoError(t, err, "build slow ffmpeg stub: %s", outBytes)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestResolveSafePath(t *testing.T) {
	tmpDir := t.TempDir()
	outsideAbs := filepath.Join(filepath.VolumeName(tmpDir)+string(filepath.Separator), "Windows", "win.ini")

	test := []struct {
		name        string
		outputRoot  string
		outputPath  string
		wantErr     bool
		skipWindows bool
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
			name:        "absolute unix-style path outside root",
			outputRoot:  tmpDir,
			outputPath:  "/etc/passwd",
			wantErr:     true,
			skipWindows: true, // на Windows "/etc/passwd" не абсолютный и Join кладёт его внутрь root
		},
		{
			name:       "absolute path outside root (volume-rooted)",
			outputRoot: tmpDir,
			outputPath: outsideAbs,
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
			if tt.skipWindows && runtime.GOOS == "windows" {
				t.Skip("unix-absolute path semantics differ on Windows")
			}
			_, err := resolveSafePath(tt.outputRoot, tt.outputPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveSavePath() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
