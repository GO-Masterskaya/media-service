package processing

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultThumbnailTimeout = 2 * time.Minute
	defaultTranscodeTimeout = 10 * time.Minute
)

// GenerateThumbnail создаёт превью через ffmpeg.
// Caller должен передавать ctx с deadline; иначе применяется внутренний таймаут 2m.
func GenerateThumbnail(ctx context.Context, inputPath, outputPath string, kind Kind, sec int) (string, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultThumbnailTimeout)
		defer cancel()
	}

	safePath, err := resolveSafePath(filepath.Dir(outputPath), outputPath)
	if err != nil {
		return "", fmt.Errorf("invalid output path: %w", err)
	}

	var args []string

	switch kind {
	case KindVideo:
		args = []string{
			"-nostdin", "-y",
			"-ss", strconv.Itoa(sec),
			"-i", inputPath,
			"-frames:v", "1",
			"-vf", "scale='min(320,iw)':-2",
			safePath,
		}
	case KindAudio:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-filter_complex", "showwavespic=s=640x120",
			"-frames:v", "1",
			safePath,
		}
	case KindImage:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-vf", "scale='min(320,iw)':-2",
			safePath,
		}
	default:
		return "", fmt.Errorf("unsupported kind: %s", kind)
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		_ = os.Remove(safePath)
		return "", fmt.Errorf("ffmpeg thumbnail failed: %w, stderr: %s", err, stderr.String())
	}

	return safePath, nil
}

// Transcode создаёт рендицию через ffmpeg.
// Caller должен передавать ctx с deadline; иначе применяется внутренний таймаут 10m.
func Transcode(ctx context.Context, inputPath, outputPath string, kind Kind) (string, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTranscodeTimeout)
		defer cancel()
	}

	safePath, err := resolveSafePath(filepath.Dir(outputPath), outputPath)
	if err != nil {
		return "", fmt.Errorf("invalid output path: %w", err)
	}

	var args []string
	switch kind {
	case KindVideo:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-vf", "scale=-2:720",
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-c:a", "aac",
			safePath,
		}
	case KindAudio:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-c:a", "aac",
			safePath,
		}
	case KindImage:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			safePath,
		}
	default:
		return "", fmt.Errorf("unsupported kind for transcode: %s", kind)
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		_ = os.Remove(safePath)
		return "", fmt.Errorf("ffmpeg transcode failed: %w, stderr: %s", err, stderr.String())
	}

	return safePath, nil
}

// resolveSafePath гарантирует, что итоговый путь находиться строго внутри outputRoot
func resolveSafePath(outputRoot, outputPath string) (string, error) {
	cleanRoot, err := filepath.EvalSymlinks(outputRoot)
	if err != nil {
		cleanRoot = filepath.Clean(outputRoot)
	}

	var target string
	if filepath.IsAbs(outputPath) {
		target = filepath.Clean(outputPath)
	} else {
		target = filepath.Clean(filepath.Join(cleanRoot, outputPath))
	}

	// Проверяем относительный путь от cleanRoot к target
	rel, err := filepath.Rel(cleanRoot, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected: %s is outside %s", outputPath, cleanRoot)
	}

	return target, nil
}
