package processing

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
)

func GenerateThumbnail(ctx context.Context, inputPath, outputPath string, kind Kind, sec int) error {
	var args []string

	switch kind {
	case KindVideo:
		args = []string{
			"-nostdin", "-y",
			"-ss", strconv.Itoa(sec),
			"-i", inputPath,
			"-frames:v", "1",
			"-vf", "scale='min(320,iw)':-2",
			outputPath,
		}
	case KindAudio:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-filter_complex", "showwavespic=s=640x120",
			"-frames:v", "1",
			outputPath,
		}
	case KindImage:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-vf", "scale='min(320,iw)':-2",
			outputPath,
		}
	default:
		return fmt.Errorf("unsupported kind: %s", kind)
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg thumbnail failed: %w", err)
	}

	return nil
}

func Transcode(ctx context.Context, inputPath, outputPath string, kind Kind) error {
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
			outputPath,
		}
	case KindAudio:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			"-c:a", "aac",
			outputPath,
		}
	case KindImage:
		args = []string{
			"-nostdin", "-y",
			"-i", inputPath,
			outputPath,
		}
	default:
		return fmt.Errorf("unsupported kind for transcode: %s", kind)
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg transcode failed: %w", err)
	}

	return nil
}
