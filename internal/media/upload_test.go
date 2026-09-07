package media

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/processing"
	"mediaservice/internal/repo"
	"mediaservice/internal/upload"
)

type mockProber struct {
	info *processing.MediaInfo
	err  error
}

func (m *mockProber) Probe(ctx context.Context, inputPath string) (*processing.MediaInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.info, nil
}

type uploadMockRepo struct {
	persistMediaRepo
	insertedJobs map[uuid.UUID][]string
}

func newUploadMockRepo() *uploadMockRepo {
	return &uploadMockRepo{
		persistMediaRepo: *newPersistMediaRepo(),
		insertedJobs:     make(map[uuid.UUID][]string),
	}
}

func (r *uploadMockRepo) InsertWithJobs(ctx context.Context, m repo.Media, jobTypes []string) (*repo.Media, error) {
	created, err := r.persistMediaRepo.InsertWithJobs(ctx, m, jobTypes)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.insertedJobs[created.ID] = jobTypes
	r.mu.Unlock()
	return created, nil
}

func setupUploadTestService(t *testing.T, prober Prober, maxUploadBytes int64, allowlist []string) (*Service, *uploadMockRepo, *countingStorage, *upload.TempStore) {
	tempDir := t.TempDir()
	tempStore, err := upload.New(upload.Config{
		Dir:             tempDir,
		MaxFileSize:     maxUploadBytes,
		ReserveBytes:    0,
		StaleGrace:      time.Hour,
		CleanupInterval: time.Hour,
	}, nil, svcTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() {
		tempStore.Stop()
	})

	mr := newUploadMockRepo()
	dr := &svcStubDerivRepo{}
	st := newCountingStorage()

	svc := NewService(mr, dr, st, 15*time.Minute, svcTestLogger())
	svc.SetUploadConfig(tempStore, prober, maxUploadBytes, allowlist)

	return svc, mr, st, tempStore
}

func chunksToReceiver(chunks ...[]byte) ChunkReceiver {
	idx := 0
	return func() ([]byte, error) {
		if idx >= len(chunks) {
			return nil, io.EOF
		}
		c := chunks[idx]
		idx++
		return c, nil
	}
}

func TestUpload_Success_Image(t *testing.T) {
	prober := &mockProber{
		info: &processing.MediaInfo{
			Kind:       processing.KindImage,
			Width:      800,
			Height:     600,
			Codec:      "png",
			FormatName: "png_pipe",
		},
	}
	svc, mr, st, tempStore := setupUploadTestService(t, prober, 1024*1024, []string{"image/*", "video/*", "audio/*"})

	ownerID := uuid.New()
	data := []byte("fake png content data 1234567890")
	params := UploadRequestParams{
		OwnerID:        ownerID,
		Filename:       "test.png",
		MIME:           "image/png",
		ExpectedSize:   uint64(len(data)),
		IdempotencyKey: "key-image-1",
		MakeThumbnail:  true,
	}

	res, err := svc.Upload(context.Background(), params, chunksToReceiver(data[:10], data[10:]))
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.NotEqual(t, uuid.Nil, res.MediaID)
	assert.False(t, res.Replay)
	assert.Equal(t, repo.MediaStatusProcessing, res.Status)

	// Check DB
	saved, err := mr.GetByID(context.Background(), res.MediaID)
	require.NoError(t, err)
	assert.Equal(t, repo.MediaKindImage, saved.Kind)
	assert.Equal(t, int64(len(data)), saved.SizeBytes)
	assert.Contains(t, string(saved.Metadata), "800")

	// Check MinIO Put was called
	assert.Equal(t, 1, st.puts)

	// Check jobs created
	mr.mu.Lock()
	jobs := mr.insertedJobs[res.MediaID]
	mr.mu.Unlock()
	assert.Equal(t, []string{"thumbnail"}, jobs)

	// Check temp file was cleaned up
	assert.Equal(t, 0, tempStore.ActiveFiles())
}

func TestUpload_Success_Video(t *testing.T) {
	prober := &mockProber{
		info: &processing.MediaInfo{
			Kind:        processing.KindVideo,
			Width:       1920,
			Height:      1080,
			Duration:    10 * time.Second,
			Codec:       "h264",
			Bitrate:     5000000,
			FormatName:  "mov,mp4,m4a,3gp,3g2,mj2",
		},
	}
	svc, mr, _, tempStore := setupUploadTestService(t, prober, 10*1024*1024, []string{"video/*"})

	ownerID := uuid.New()
	data := []byte("video data chunk bytes")
	params := UploadRequestParams{
		OwnerID:        ownerID,
		Filename:       "video.mp4",
		MIME:           "video/mp4",
		ExpectedSize:   uint64(len(data)),
		IdempotencyKey: "key-video-1",
		MakeThumbnail:  true,
		Transcode:      true,
	}

	res, err := svc.Upload(context.Background(), params, chunksToReceiver(data))
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, repo.MediaStatusProcessing, res.Status)

	mr.mu.Lock()
	jobs := mr.insertedJobs[res.MediaID]
	mr.mu.Unlock()
	assert.ElementsMatch(t, []string{"thumbnail", "transcode"}, jobs)
	assert.Equal(t, 0, tempStore.ActiveFiles())
}

func TestUpload_Success_Audio(t *testing.T) {
	prober := &mockProber{
		info: &processing.MediaInfo{
			Kind:          processing.KindAudio,
			Duration:      60 * time.Second,
			AudioChannels: 2,
			AudioCodec:    "mp3",
			Bitrate:       320000,
			FormatName:    "mp3",
		},
	}
	svc, mr, _, tempStore := setupUploadTestService(t, prober, 10*1024*1024, []string{"audio/*"})

	ownerID := uuid.New()
	data := []byte("audio mp3 data bytes")
	params := UploadRequestParams{
		OwnerID:        ownerID,
		Filename:       "track.mp3",
		MIME:           "audio/mp3",
		ExpectedSize:   uint64(len(data)),
		IdempotencyKey: "key-audio-1",
		Transcode:      true,
	}

	res, err := svc.Upload(context.Background(), params, chunksToReceiver(data))
	require.NoError(t, err)
	assert.NotNil(t, res)

	mr.mu.Lock()
	jobs := mr.insertedJobs[res.MediaID]
	mr.mu.Unlock()
	assert.Equal(t, []string{"transcode"}, jobs)
	assert.Equal(t, 0, tempStore.ActiveFiles())
}

func TestUpload_ValidationErrors(t *testing.T) {
	prober := &mockProber{
		info: &processing.MediaInfo{Kind: processing.KindImage},
	}
	svc, _, _, _ := setupUploadTestService(t, prober, 1024, []string{"image/png"})
	validOwner := uuid.New()

	t.Run("missing owner_id", func(t *testing.T) {
		params := UploadRequestParams{
			OwnerID:        uuid.Nil,
			IdempotencyKey: "k1",
			MIME:           "image/png",
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("a")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})

	t.Run("caller mismatch", func(t *testing.T) {
		caller := uuid.New()
		params := UploadRequestParams{
			OwnerID:        validOwner,
			CallerID:       caller,
			IdempotencyKey: "k1",
			MIME:           "image/png",
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("a")))
		assert.ErrorIs(t, err, ErrAccessDenied)
	})

	t.Run("missing idempotency_key", func(t *testing.T) {
		params := UploadRequestParams{
			OwnerID:        validOwner,
			IdempotencyKey: "",
			MIME:           "image/png",
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("a")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})

	t.Run("disallowed mime type", func(t *testing.T) {
		params := UploadRequestParams{
			OwnerID:        validOwner,
			IdempotencyKey: "k1",
			MIME:           "application/pdf",
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("a")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})

	t.Run("expected size exceeds maxUploadBytes", func(t *testing.T) {
		params := UploadRequestParams{
			OwnerID:        validOwner,
			IdempotencyKey: "k1",
			MIME:           "image/png",
			ExpectedSize:   2048,
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("a")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})

	t.Run("expiration time in the past", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Hour)
		params := UploadRequestParams{
			OwnerID:        validOwner,
			IdempotencyKey: "k1",
			MIME:           "image/png",
			ExpiresAt:      &past,
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("a")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})

	t.Run("empty upload body", func(t *testing.T) {
		params := UploadRequestParams{
			OwnerID:        validOwner,
			IdempotencyKey: "k1",
			MIME:           "image/png",
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver())
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})

	t.Run("actual size mismatch with expected size", func(t *testing.T) {
		params := UploadRequestParams{
			OwnerID:        validOwner,
			IdempotencyKey: "k1",
			MIME:           "image/png",
			ExpectedSize:   100,
		}
		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("only 10 bytes")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})
}

func TestUpload_SizeLimitExceeded(t *testing.T) {
	prober := &mockProber{info: &processing.MediaInfo{Kind: processing.KindImage}}
	// max 20 bytes
	svc, _, _, tempStore := setupUploadTestService(t, prober, 20, []string{"image/*"})

	params := UploadRequestParams{
		OwnerID:        uuid.New(),
		IdempotencyKey: "key-limit",
		MIME:           "image/png",
	}

	largeChunk := make([]byte, 50)
	_, err := svc.Upload(context.Background(), params, chunksToReceiver(largeChunk))
	assert.ErrorIs(t, err, ErrInvalidArgument)
	assert.Equal(t, 0, tempStore.ActiveFiles())
}

func TestUpload_MIMEClassMismatch(t *testing.T) {
	t.Run("declared image but probed video", func(t *testing.T) {
		prober := &mockProber{
			info: &processing.MediaInfo{Kind: processing.KindVideo},
		}
		svc, _, _, tempStore := setupUploadTestService(t, prober, 1024, []string{"image/*", "video/*"})

		params := UploadRequestParams{
			OwnerID:        uuid.New(),
			IdempotencyKey: "fake-img",
			MIME:           "image/png",
		}

		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("some data")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
		assert.Contains(t, err.Error(), "mime class")
		assert.Equal(t, 0, tempStore.ActiveFiles())
	})

	t.Run("declared video but probed audio", func(t *testing.T) {
		prober := &mockProber{
			info: &processing.MediaInfo{Kind: processing.KindAudio},
		}
		svc, _, _, tempStore := setupUploadTestService(t, prober, 1024, []string{"image/*", "video/*", "audio/*"})

		params := UploadRequestParams{
			OwnerID:        uuid.New(),
			IdempotencyKey: "fake-vid",
			MIME:           "video/mp4",
		}

		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("some data")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
		assert.Equal(t, 0, tempStore.ActiveFiles())
	})
}

func TestUpload_CorruptMedia(t *testing.T) {
	prober := &mockProber{
		err: errors.New("ffprobe: invalid data found when processing input"),
	}
	svc, _, st, tempStore := setupUploadTestService(t, prober, 1024, []string{"image/*"})

	params := UploadRequestParams{
		OwnerID:        uuid.New(),
		IdempotencyKey: "corrupt-file",
		MIME:           "image/jpeg",
	}

	_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("corrupted byte sequence")))
	assert.ErrorIs(t, err, ErrInvalidArgument)
	assert.Contains(t, err.Error(), "corrupt or unreadable")
	// MinIO Put should NOT be called for corrupt media
	assert.Equal(t, 0, st.puts)
	assert.Equal(t, 0, tempStore.ActiveFiles())
}

func TestUpload_ProcessingOptionsValidation(t *testing.T) {
	t.Run("thumbnail on audio rejected", func(t *testing.T) {
		prober := &mockProber{info: &processing.MediaInfo{Kind: processing.KindAudio}}
		svc, _, _, tempStore := setupUploadTestService(t, prober, 1024, []string{"audio/*"})

		params := UploadRequestParams{
			OwnerID:        uuid.New(),
			IdempotencyKey: "audio-thumb",
			MIME:           "audio/mp3",
			MakeThumbnail:  true,
		}

		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("mp3 content")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
		assert.Contains(t, err.Error(), "thumbnail is not supported for audio")
		assert.Equal(t, 0, tempStore.ActiveFiles())
	})

	t.Run("transcode on image rejected", func(t *testing.T) {
		prober := &mockProber{info: &processing.MediaInfo{Kind: processing.KindImage}}
		svc, _, _, tempStore := setupUploadTestService(t, prober, 1024, []string{"image/*"})

		params := UploadRequestParams{
			OwnerID:        uuid.New(),
			IdempotencyKey: "img-transcode",
			MIME:           "image/png",
			Transcode:      true,
		}

		_, err := svc.Upload(context.Background(), params, chunksToReceiver([]byte("png content")))
		assert.ErrorIs(t, err, ErrInvalidArgument)
		assert.Contains(t, err.Error(), "transcode is not supported for images")
		assert.Equal(t, 0, tempStore.ActiveFiles())
	})
}

func TestUpload_IdempotencyReplay_And_Conflict(t *testing.T) {
	prober := &mockProber{info: &processing.MediaInfo{Kind: processing.KindImage}}
	svc, _, _, tempStore := setupUploadTestService(t, prober, 1024, []string{"image/*"})

	ownerID := uuid.New()
	data := []byte("idempotent png payload")
	params := UploadRequestParams{
		OwnerID:        ownerID,
		Filename:       "orig.png",
		MIME:           "image/png",
		IdempotencyKey: "replay-key-1",
	}

	// 1st Upload
	res1, err := svc.Upload(context.Background(), params, chunksToReceiver(data))
	require.NoError(t, err)
	assert.False(t, res1.Replay)

	// 2nd Upload (Replay with same body & params)
	res2, err := svc.Upload(context.Background(), params, chunksToReceiver(data))
	require.NoError(t, err)
	assert.True(t, res2.Replay)
	assert.Equal(t, res1.MediaID, res2.MediaID)

	// 3rd Upload (Conflict with different body)
	differentData := []byte("different payload content")
	_, err = svc.Upload(context.Background(), params, chunksToReceiver(differentData))
	assert.ErrorIs(t, err, ErrAlreadyExists)

	assert.Equal(t, 0, tempStore.ActiveFiles())
}

func TestValidateMIME(t *testing.T) {
	allowlist := []string{"image/*", "video/mp4", "audio/mp3"}

	assert.True(t, ValidateMIME("image/png", allowlist))
	assert.True(t, ValidateMIME("image/jpeg", allowlist))
	assert.True(t, ValidateMIME("video/mp4", allowlist))
	assert.True(t, ValidateMIME("audio/mp3", allowlist))

	assert.False(t, ValidateMIME("video/mkv", allowlist))
	assert.False(t, ValidateMIME("audio/wav", allowlist))
	assert.False(t, ValidateMIME("application/json", allowlist))
	assert.False(t, ValidateMIME("", allowlist))

	// Empty allowlist allows everything
	assert.True(t, ValidateMIME("any/thing", nil))
	assert.True(t, ValidateMIME("any/thing", []string{}))

	// Wildcard wildcard
	assert.True(t, ValidateMIME("application/zip", []string{"*"}))
	assert.True(t, ValidateMIME("application/zip", []string{"*/*"}))
}
