package api

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"mediaservice/internal/media"
	"mediaservice/internal/processing"
	"mediaservice/internal/repo"
	"mediaservice/internal/upload"
	mediav1 "mediaservice/proto/media/v1"
)

type mockUploadServerStream struct {
	grpc.ServerStream
	ctx      context.Context
	requests []*mediav1.UploadRequest
	idx      int
	response *mediav1.UploadResponse
}

func newMockUploadServerStream(ctx context.Context, reqs ...*mediav1.UploadRequest) *mockUploadServerStream {
	return &mockUploadServerStream{
		ctx:      ctx,
		requests: reqs,
	}
}

func (m *mockUploadServerStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *mockUploadServerStream) Recv() (*mediav1.UploadRequest, error) {
	if m.idx >= len(m.requests) {
		return nil, io.EOF
	}
	req := m.requests[m.idx]
	m.idx++
	return req, nil
}

func (m *mockUploadServerStream) SendAndClose(resp *mediav1.UploadResponse) error {
	m.response = resp
	return nil
}

type mockUploadProber struct {
	info *processing.MediaInfo
	err  error
}

func (m *mockUploadProber) Probe(ctx context.Context, inputPath string) (*processing.MediaInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.info, nil
}

type apiUploadMediaRepo struct {
	mu     sync.Mutex
	byKey  map[string]*repo.Media
	byID   map[uuid.UUID]*repo.Media
	jobsOf map[uuid.UUID][]string
}

func newAPIUploadMediaRepo() *apiUploadMediaRepo {
	return &apiUploadMediaRepo{
		byKey:  make(map[string]*repo.Media),
		byID:   make(map[uuid.UUID]*repo.Media),
		jobsOf: make(map[uuid.UUID][]string),
	}
}

func (r *apiUploadMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.byID[id]
	if !ok {
		return nil, repo.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (r *apiUploadMediaRepo) GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*repo.Media, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := ownerID.String() + "|" + idempotencyKey
	m, ok := r.byKey[k]
	if !ok {
		return nil, repo.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (r *apiUploadMediaRepo) InsertWithJobs(ctx context.Context, m repo.Media, jobTypes []string) (*repo.Media, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := m.OwnerID.String() + "|" + m.IdempotencyKey
	if _, exists := r.byKey[k]; exists {
		return nil, repo.ErrConcurrentConflict
	}
	if len(jobTypes) > 0 {
		m.Status = repo.MediaStatusProcessing
	} else {
		m.Status = repo.MediaStatusStored
	}
	cp := m
	r.byKey[k] = &cp
	r.byID[m.ID] = &cp
	r.jobsOf[m.ID] = jobTypes
	out := cp
	return &out, nil
}

func (r *apiUploadMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	return nil, nil
}
func (r *apiUploadMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error { return nil }
func (r *apiUploadMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	return map[uuid.UUID]struct{}{}, nil
}
func (r *apiUploadMediaRepo) MarkDeleting(ctx context.Context, id uuid.UUID) (*repo.Media, repo.ClaimState, error) {
	return nil, repo.ClaimNone, nil
}
func (r *apiUploadMediaRepo) ListDeletableByOwner(ctx context.Context, ownerID uuid.UUID, limit int) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *apiUploadMediaRepo) ListExpiredIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *apiUploadMediaRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	return nil
}
func (r *apiUploadMediaRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (int, error) {
	return 0, nil
}

func setupAPITestServer(t *testing.T, prober media.Prober, strictOwner bool) (*MediaServer, *media.Service, *apiUploadMediaRepo, *stubStorage) {
	tempDir := t.TempDir()
	tempStore, err := upload.New(upload.Config{
		Dir:             tempDir,
		MaxFileSize:     10 * 1024 * 1024,
		ReserveBytes:    0,
		StaleGrace:      time.Hour,
		CleanupInterval: time.Hour,
	}, nil, testLogger())
	require.NoError(t, err)
	t.Cleanup(func() {
		tempStore.Stop()
	})

	mr := newAPIUploadMediaRepo()
	dr := &stubDerivRepo{}
	st := &stubStorage{}

	svc := media.NewService(mr, dr, st, 15*time.Minute, testLogger())
	svc.SetUploadConfig(tempStore, prober, 10*1024*1024, []string{"image/*", "video/*", "audio/*"})

	server := NewMediaServer(svc, strictOwner)
	return server, svc, mr, st
}

func TestUpload_Success(t *testing.T) {
	prober := &mockUploadProber{
		info: &processing.MediaInfo{
			Kind:       processing.KindImage,
			Width:      640,
			Height:     480,
			Codec:      "png",
			FormatName: "png",
		},
	}
	server, _, _, _ := setupAPITestServer(t, prober, false)

	ownerID := uuid.New().String()
	stream := newMockUploadServerStream(
		context.Background(),
		&mediav1.UploadRequest{
			Payload: &mediav1.UploadRequest_Init{
				Init: &mediav1.UploadInit{
					OwnerId:        ownerID,
					Filename:       "pic.png",
					Mime:           "image/png",
					IdempotencyKey: "idem-api-1",
					ExpectedSize:   12,
					Processing: &mediav1.ProcessingOptions{
						MakeThumbnail: true,
					},
					Ttl: durationpb.New(24 * time.Hour),
				},
			},
		},
		&mediav1.UploadRequest{
			Payload: &mediav1.UploadRequest_Chunk{
				Chunk: []byte("hello world!"),
			},
		},
	)

	err := server.Upload(stream)
	require.NoError(t, err)
	require.NotNil(t, stream.response)
	assert.NotEmpty(t, stream.response.MediaId)
	assert.Equal(t, mediav1.MediaStatus_PROCESSING, stream.response.Status)
}

func TestUpload_ProtocolOrderViolations(t *testing.T) {
	prober := &mockUploadProber{info: &processing.MediaInfo{Kind: processing.KindImage}}
	server, _, _, _ := setupAPITestServer(t, prober, false)

	t.Run("empty stream", func(t *testing.T) {
		stream := newMockUploadServerStream(context.Background())
		err := server.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "stream is empty")
	})

	t.Run("chunk before init", func(t *testing.T) {
		stream := newMockUploadServerStream(
			context.Background(),
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Chunk{
					Chunk: []byte("early chunk"),
				},
			},
		)
		err := server.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "first message must be UploadInit")
	})

	t.Run("duplicate init in stream", func(t *testing.T) {
		ownerID := uuid.New().String()
		initMsg := &mediav1.UploadRequest{
			Payload: &mediav1.UploadRequest_Init{
				Init: &mediav1.UploadInit{
					OwnerId:        ownerID,
					Mime:           "image/png",
					IdempotencyKey: "idem-dup",
				},
			},
		}
		stream := newMockUploadServerStream(
			context.Background(),
			initMsg,
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Chunk{Chunk: []byte("chunk 1")},
			},
			initMsg, // duplicate init
		)
		err := server.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "duplicate UploadInit")
	})
}

func TestUpload_ValidationErrors(t *testing.T) {
	prober := &mockUploadProber{info: &processing.MediaInfo{Kind: processing.KindImage}}
	server, _, _, _ := setupAPITestServer(t, prober, false)

	t.Run("invalid owner uuid", func(t *testing.T) {
		stream := newMockUploadServerStream(
			context.Background(),
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Init{
					Init: &mediav1.UploadInit{
						OwnerId:        "not-a-uuid",
						Mime:           "image/png",
						IdempotencyKey: "k1",
					},
				},
			},
		)
		err := server.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("negative ttl", func(t *testing.T) {
		stream := newMockUploadServerStream(
			context.Background(),
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Init{
					Init: &mediav1.UploadInit{
						OwnerId:        uuid.New().String(),
						Mime:           "image/png",
						IdempotencyKey: "k1",
						Ttl:            durationpb.New(-5 * time.Minute),
					},
				},
			},
		)
		err := server.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func TestUpload_CallerIdentity(t *testing.T) {
	prober := &mockUploadProber{info: &processing.MediaInfo{Kind: processing.KindImage}}

	t.Run("strict owner check missing caller returns Unauthenticated", func(t *testing.T) {
		strictServer, _, _, _ := setupAPITestServer(t, prober, true)
		stream := newMockUploadServerStream(
			context.Background(), // no metadata
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Init{
					Init: &mediav1.UploadInit{
						OwnerId:        uuid.New().String(),
						Mime:           "image/png",
						IdempotencyKey: "k1",
					},
				},
			},
		)
		err := strictServer.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("caller id mismatch with owner id returns PermissionDenied", func(t *testing.T) {
		server, _, _, _ := setupAPITestServer(t, prober, false)
		callerID := uuid.New()
		ownerID := uuid.New()

		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-owner-id", callerID.String()))
		stream := newMockUploadServerStream(
			ctx,
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Init{
					Init: &mediav1.UploadInit{
						OwnerId:        ownerID.String(),
						Mime:           "image/png",
						IdempotencyKey: "k1",
					},
				},
			},
			&mediav1.UploadRequest{
				Payload: &mediav1.UploadRequest_Chunk{Chunk: []byte("test")},
			},
		)
		err := server.Upload(stream)
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
}
