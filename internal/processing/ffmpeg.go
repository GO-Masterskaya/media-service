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
	defaultRendition        = 720
)

// GenerateThumbnail создаёт превью через ffmpeg.
// timeout применяется поверх родительского контекста; если timeout <= 0,
// используется внутренний дефолт.
func GenerateThumbnail(ctx context.Context, outputRoot, inputPath, outputPath string, kind Kind, sec int, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = defaultThumbnailTimeout
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	safePath, err := resolveSafePath(outputRoot, outputPath)
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
		return "", fmt.Errorf("ffmpeg thumbnail failed: %w, stderr: %s", wrapCmdErr(ctx, err), stderr.String())
	}

	return safePath, nil
}

// Transcode создаёт рендицию через ffmpeg.
// timeout применяется поверх родительского контекста; если timeout <= 0,
// используется внутренний дефолт. rendition <= 0 → defaultRendition.
func Transcode(ctx context.Context, outputRoot, inputPath, outputPath string, kind Kind, rendition int, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = defaultTranscodeTimeout
	}
	if rendition <= 0 {
		rendition = defaultRendition
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	safePath, err := resolveSafePath(outputRoot, outputPath)
	if err != nil {
		return "", fmt.Errorf("invalid output path: %w", err)
	}

	var args []string
	switch kind {
	case KindVideo:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-vf", fmt.Sprintf("scale=-2:%d", rendition),
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
		return "", fmt.Errorf("ffmpeg transcode failed: %w, stderr: %s", wrapCmdErr(ctx, err), stderr.String())
	}

	return safePath, nil
}

// wrapCmdErr добавляет ctx.Err() поверх ошибки процесса, чтобы таймаут
// был отличим от обычного падения ffmpeg (signal: killed без контекста).
func wrapCmdErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}
	return err
}

// resolveSafePath гарантирует, что итоговый путь находиться строго внутри outputRoot
func resolveSafePath(outputRoot, outputPath string) (string, error) {
	cleanRoot := filepath.Clean(outputRoot)
	if r, err := filepath.EvalSymlinks(cleanRoot); err == nil {
		cleanRoot = r
	}

	target := outputPath
	if !filepath.IsAbs(target) {
		target = filepath.Join(cleanRoot, target)
	}
	target = filepath.Clean(target)

	// Самого файла ещё нет, поэтому разворачиваем его каталог.
	if parent, err := filepath.EvalSymlinks(filepath.Dir(target)); err == nil {
		target = filepath.Join(parent, filepath.Base(target))
	}

	rel, err := filepath.Rel(cleanRoot, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected: %s is outside %s", outputPath, cleanRoot)
	}
	return target, nil
}
