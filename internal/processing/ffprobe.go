package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

type Kind string

const (
	KindImage Kind = "image"
	KindVideo Kind = "video"
	KindAudio Kind = "audio"
)

type ffprobeStream struct {
	CodecType string `json:"codec_type"`
	CodecName string `json:"codec_name"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	Duration  string `json:"duration"`
	BitRate   string `json:"bit_rate"`
	NbFrames  string `json:"nb_frames"`
	Channels  int    `json:"channels"`
}

type ffprobeFormat struct {
	FormatName string `json:"format_name"`
	Duration   string `json:"duration"`
	BitRate    string `json:"bit_rate"`
}

type ffprobeOutput struct {
	Streams []ffprobeStream `json:"streams"`
	Format  ffprobeFormat   `json:"format"`
}

type MediaInfo struct {
	Kind          Kind
	Duration      time.Duration
	Width         int
	Height        int
	Codec         string
	Bitrate       int64
	FrameCount    int64
	AudioChannels int
	FormatName    string
}

func Probe(ctx context.Context, inputPath string) (*MediaInfo, error) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "quiet", "-print_format",
		"json", "-show_format", "-show_streams", inputPath)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var raw ffprobeOutput
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("ffprobe json parse: %w", err)
	}

	info := &MediaInfo{}

	var hasVideo, hasAudio bool

	for _, s := range raw.Streams {
		switch s.CodecType{
		case "video":
			hasVideo = true
			info.Width = s.Width
			info.Height = s.Height
			info.Codec = s.CodecName
			if s.NbFrames != "" {
				fc, err := strconv.ParseInt(s.NbFrames, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("error parsing NbFrames: %w", err)

				}
				info.FrameCount = fc
			}
		case "audio":
			hasAudio = true
			info.AudioChannels = s.Channels
			if info.Codec == "" {
				info.Codec = s.CodecName
			}
		}
	}

	info.FormatName = raw.Format.FormatName

	if raw.Format.Duration != "" {
		sec, err := strconv.ParseFloat(raw.Format.Duration, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing info.Duration: %w", err)
		}
		info.Duration = time.Duration(sec * float64(time.Second))
	}

	if raw.Format.BitRate != "" {
		br, err := strconv.ParseInt(raw.Format.BitRate, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing info.Bitrate: %w", err)
		}
		info.Bitrate = br
	}

	switch {
	case hasVideo && hasAudio:
		info.Kind = KindVideo
	case hasVideo && !hasAudio:
		if info.FrameCount <= 1 && info.Duration == 0 {
			info.Kind = KindImage
		} else {
			info.Kind = KindVideo
		}
	case !hasVideo && hasAudio:
		info.Kind = KindAudio
	default:
		return nil, fmt.Errorf("no media streams found in %s", inputPath)
	}

	return info, nil
}
