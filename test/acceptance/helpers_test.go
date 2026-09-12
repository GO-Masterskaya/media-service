package acceptance_test

import (
	"bytes"
	_ "embed"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	mediav1 "mediaservice/proto/media/v1"
)

//go:embed testdata/pixel64.png
var png64 []byte

// exeBody — тело с MZ-заголовком (подделка под image/png).
var exeBody = append([]byte{'M', 'Z'}, bytes.Repeat([]byte{0x00}, 64)...)

type uploadOpts struct {
	filename       string
	mime           string
	idempotencyKey string
	makeThumbnail  bool
	transcode      bool
	ttl            time.Duration
	body           []byte
}

func (s *AcceptanceSuite) upload(t *testing.T, opts uploadOpts) *mediav1.UploadResponse {
	t.Helper()
	if opts.body == nil {
		opts.body = png64
	}
	if opts.filename == "" {
		opts.filename = "pixel.png"
	}
	if opts.mime == "" {
		opts.mime = "image/png"
	}
	if opts.idempotencyKey == "" {
		opts.idempotencyKey = uuid.NewString()
	}

	stream, err := s.client.Upload(s.authCtx())
	require.NoError(t, err)

	initMsg := &mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Init{Init: &mediav1.UploadInit{
			OwnerId:        s.ownerID.String(),
			Filename:       opts.filename,
			Mime:           opts.mime,
			ExpectedSize:   uint64(len(opts.body)),
			IdempotencyKey: opts.idempotencyKey,
			Processing: &mediav1.ProcessingOptions{
				MakeThumbnail: opts.makeThumbnail,
				Transcode:     opts.transcode,
			},
		}},
	}
	if opts.ttl > 0 {
		initMsg.GetInit().Ttl = durationpb.New(opts.ttl)
	}
	require.NoError(t, stream.Send(initMsg))

	const chunkSize = 16 * 1024
	for off := 0; off < len(opts.body); off += chunkSize {
		end := off + chunkSize
		if end > len(opts.body) {
			end = len(opts.body)
		}
		require.NoError(t, stream.Send(&mediav1.UploadRequest{
			Payload: &mediav1.UploadRequest_Chunk{Chunk: opts.body[off:end]},
		}))
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.NotEmpty(t, resp.MediaId)
	return resp
}

func (s *AcceptanceSuite) waitStatus(t *testing.T, mediaID string, want ...mediav1.MediaStatus) *mediav1.Media {
	t.Helper()
	wantSet := make(map[mediav1.MediaStatus]struct{}, len(want))
	for _, st := range want {
		wantSet[st] = struct{}{}
	}

	deadline := time.Now().Add(90 * time.Second)
	var last *mediav1.Media
	for time.Now().Before(deadline) {
		m, err := s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{MediaId: mediaID})
		require.NoError(t, err)
		last = m
		if _, ok := wantSet[m.Status]; ok {
			return m
		}
		if m.Status == mediav1.MediaStatus_FAILED {
			t.Fatalf("media %s failed: %v", mediaID, m.Error)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for status %v, last=%v", want, last.GetStatus())
	return nil
}

func (s *AcceptanceSuite) downloadStreamAll(t *testing.T, mediaID, variant string) []byte {
	t.Helper()
	stream, err := s.client.DownloadStream(s.authCtx(), &mediav1.DownloadStreamRequest{
		MediaId: mediaID,
		Variant: variant,
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		_, _ = buf.Write(chunk.Data)
	}
	return buf.Bytes()
}

func requireGRPCCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "want gRPC status, got %v", err)
	require.Equal(t, code, st.Code(), "status message: %s", st.Message())
}
