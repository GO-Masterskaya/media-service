package media

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"mediaservice/internal/repo"
	"mediaservice/internal/storage"
)

type persistMediaRepo struct {
	mu    sync.Mutex
	byKey map[string]*repo.Media
	fail  error // InsertWithJobs returns this after first call attempt
}

func newPersistMediaRepo() *persistMediaRepo {
	return &persistMediaRepo{byKey: make(map[string]*repo.Media)}
}

func (r *persistMediaRepo) key(owner uuid.UUID, idem string) string {
	return owner.String() + "|" + idem
}

func (r *persistMediaRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.byKey {
		if m.ID == id {
			cp := *m
			return &cp, nil
		}
	}
	return nil, repo.ErrNotFound
}

func (r *persistMediaRepo) GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*repo.Media, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.byKey[r.key(ownerID, idempotencyKey)]
	if !ok {
		return nil, repo.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (r *persistMediaRepo) InsertWithJobs(ctx context.Context, m repo.Media, jobTypes []string) (*repo.Media, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		err := r.fail
		r.fail = nil
		return nil, err
	}
	k := r.key(m.OwnerID, m.IdempotencyKey)
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
	out := cp
	return &out, nil
}

func (r *persistMediaRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	return nil, nil
}
func (r *persistMediaRepo) HardDelete(ctx context.Context, id uuid.UUID) error { return nil }
func (r *persistMediaRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	return map[uuid.UUID]struct{}{}, nil
}

func (s *persistMediaRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	return nil
}

func (s *persistMediaRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (usagesRemaining int, err error) {
	return 0, nil
}

type countingStorage struct {
	mu             sync.Mutex
	puts           int
	deletes        int
	objects        map[string][]byte
	putErr         error
	cancelAfterPut context.CancelFunc
}

func newCountingStorage() *countingStorage {
	return &countingStorage{objects: make(map[string][]byte)}
}

func (s *countingStorage) PutObject(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return s.putErr
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.puts++
	s.objects[key] = b
	if s.cancelAfterPut != nil {
		s.cancelAfterPut()
	}
	return nil
}

func (s *countingStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}
func (s *countingStorage) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (*storage.PresignedURL, error) {
	return nil, errors.New("not implemented")
}
func (s *countingStorage) DeleteObject(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	delete(s.objects, key)
	return nil
}
func (s *countingStorage) DeletePrefix(ctx context.Context, prefix string) error { return nil }
func (s *countingStorage) ForEachObject(ctx context.Context, prefix string, fn func(storage.ObjectInfo) error) error {
	return nil
}
func (s *countingStorage) Close() error { return nil }

func TestPersistUpload_firstCreatesMedia(t *testing.T) {
	repoStub := newPersistMediaRepo()
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	body := []byte("png-bytes")
	mediaID := uuid.New()
	in := PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           mediaID,
		IdempotencyKey:    "k1",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         int64(len(body)),
		BodyFingerprint:   "bf-1",
		ParamsFingerprint: ParamsFingerprint("image/png", true, false, nil),
		JobTypes:          []string{"thumbnail"},
		Reader:            bytes.NewReader(body),
	}

	res, err := svc.PersistUpload(context.Background(), in)
	require.NoError(t, err)
	require.False(t, res.Replay)
	require.Equal(t, mediaID, res.Media.ID)
	require.Equal(t, repo.MediaStatusProcessing, res.Media.Status)
	require.Equal(t, 1, sto.puts)
	require.Equal(t, 0, sto.deletes)
}

func TestPersistUpload_requiresMediaID(t *testing.T) {
	svc := NewService(newPersistMediaRepo(), &svcStubDerivRepo{}, newCountingStorage(), time.Minute, svcTestLogger())
	_, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           ownerID(),
		IdempotencyKey:    "k0",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         1,
		BodyFingerprint:   "bf",
		ParamsFingerprint: ParamsFingerprint("image/png", false, false, nil),
		Reader:            bytes.NewReader([]byte("x")),
	})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestPersistUpload_replaySameFingerprints(t *testing.T) {
	repoStub := newPersistMediaRepo()
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	body := []byte("png-bytes")
	params := ParamsFingerprint("image/png", false, false, nil)
	in := PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k2",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         int64(len(body)),
		BodyFingerprint:   "bf-2",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader(body),
	}

	first, err := svc.PersistUpload(context.Background(), in)
	require.NoError(t, err)

	in.Reader = bytes.NewReader(body)
	second, err := svc.PersistUpload(context.Background(), in)
	require.NoError(t, err)
	require.True(t, second.Replay)
	require.Equal(t, first.Media.ID, second.Media.ID)
	require.Equal(t, 1, sto.puts, "replay must not put again")
}

func TestPersistUpload_conflictDifferentBody(t *testing.T) {
	repoStub := newPersistMediaRepo()
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	params := ParamsFingerprint("image/png", false, false, nil)
	in := PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k3",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         3,
		BodyFingerprint:   "bf-old",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("abc")),
	}
	_, err := svc.PersistUpload(context.Background(), in)
	require.NoError(t, err)

	in.BodyFingerprint = "bf-new"
	in.Reader = bytes.NewReader([]byte("xyz"))
	_, err = svc.PersistUpload(context.Background(), in)
	require.ErrorIs(t, err, ErrAlreadyExists)
	require.Equal(t, 1, sto.puts)
}

func TestPersistUpload_legacyEmptyFingerprintsReplay(t *testing.T) {
	repoStub := newPersistMediaRepo()
	params := ParamsFingerprint("image/png", false, false, nil)
	repoStub.byKey[repoStub.key(ownerID(), "k-legacy")] = &repo.Media{
		ID:                uuid.New(),
		OwnerID:           ownerID(),
		IdempotencyKey:    "k-legacy",
		BodyFingerprint:   "",
		ParamsFingerprint: "",
		Status:            repo.MediaStatusStored,
		StorageKey:        "legacy/original.png",
	}
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	res, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k-legacy",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         3,
		BodyFingerprint:   "bf-new",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("abc")),
	})
	require.NoError(t, err)
	require.True(t, res.Replay)
	require.Equal(t, 0, sto.puts)
}

func TestPersistUpload_rejectsUnknownJobType(t *testing.T) {
	svc := NewService(newPersistMediaRepo(), &svcStubDerivRepo{}, newCountingStorage(), time.Minute, svcTestLogger())
	_, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k-job",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         1,
		BodyFingerprint:   "bf",
		ParamsFingerprint: ParamsFingerprint("image/png", false, false, nil),
		JobTypes:          []string{"thumbnail", "nope"},
		Reader:            bytes.NewReader([]byte("x")),
	})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestPersistUpload_replayRejectsDeletingStatus(t *testing.T) {
	repoStub := newPersistMediaRepo()
	params := ParamsFingerprint("image/png", false, false, nil)
	repoStub.byKey[repoStub.key(ownerID(), "k-del")] = &repo.Media{
		ID:                uuid.New(),
		OwnerID:           ownerID(),
		IdempotencyKey:    "k-del",
		BodyFingerprint:   "bf-del",
		ParamsFingerprint: params,
		Status:            repo.MediaStatusDeleting,
		StorageKey:        "x/original.png",
	}
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	_, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k-del",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         1,
		BodyFingerprint:   "bf-del",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("x")),
	})
	require.ErrorIs(t, err, ErrFailedPrecondition)
	require.Equal(t, 0, sto.puts)
}

func TestPersistUpload_idTakenBeforePutDoesNotOverwrite(t *testing.T) {
	params := ParamsFingerprint("image/png", false, false, nil)
	mediaID := uuid.New()
	repoStub := newPersistMediaRepo()
	repoStub.byKey[repoStub.key(ownerID(), "other-key")] = &repo.Media{
		ID:                mediaID,
		OwnerID:           ownerID(),
		IdempotencyKey:    "other-key",
		BodyFingerprint:   "bf-other",
		ParamsFingerprint: params,
		Status:            repo.MediaStatusStored,
		StorageKey:        "owner/" + mediaID.String() + "/original.png",
	}
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	_, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           mediaID,
		IdempotencyKey:    "new-key",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-new",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("data")),
	})
	require.ErrorIs(t, err, ErrAlreadyExists)
	require.Equal(t, 0, sto.puts, "must not PutObject over existing media_id")
}

func TestPersistUpload_compensateDeleteOnDBFailure(t *testing.T) {
	repoStub := newPersistMediaRepo()
	repoStub.fail = errors.New("db down")
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	in := PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k4",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-4",
		ParamsFingerprint: ParamsFingerprint("image/png", false, false, nil),
		Reader:            bytes.NewReader([]byte("data")),
	}
	_, err := svc.PersistUpload(context.Background(), in)
	require.Error(t, err)
	require.Equal(t, 1, sto.puts)
	require.Equal(t, 1, sto.deletes)
	require.Empty(t, sto.objects)
}

func TestPersistUpload_compensateUsesDetachedContext(t *testing.T) {
	repoStub := newPersistMediaRepo()
	repoStub.fail = errors.New("db down")
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	ctx, cancel := context.WithCancel(context.Background())
	sto.cancelAfterPut = cancel

	in := PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k4-cancel",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-4c",
		ParamsFingerprint: ParamsFingerprint("image/png", false, false, nil),
		Reader:            bytes.NewReader([]byte("data")),
	}

	_, err := svc.PersistUpload(ctx, in)
	require.Error(t, err)
	require.Equal(t, 1, sto.puts)
	require.Equal(t, 1, sto.deletes, "compensate DeleteObject must run on detached ctx")
	require.Empty(t, sto.objects)
}

func TestPersistUpload_concurrentConflictResolvesToReplay(t *testing.T) {
	repoStub := newPersistMediaRepo()
	// Pre-seed winner row as if another request committed first.
	winnerID := uuid.New()
	params := ParamsFingerprint("image/png", false, false, nil)
	repoStub.byKey[repoStub.key(ownerID(), "k5")] = &repo.Media{
		ID:                winnerID,
		OwnerID:           ownerID(),
		IdempotencyKey:    "k5",
		BodyFingerprint:   "bf-5",
		ParamsFingerprint: params,
		Status:            repo.MediaStatusStored,
		StorageKey:        "x/y/original.png",
	}

	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	in := PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k5",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-5",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("data")),
	}
	res, err := svc.PersistUpload(context.Background(), in)
	require.NoError(t, err)
	require.True(t, res.Replay)
	require.Equal(t, winnerID, res.Media.ID)
	require.Equal(t, 0, sto.puts)
}

func TestPersistUpload_raceAfterPutSameKeyDoesNotDelete(t *testing.T) {
	// Штатный ретрай: тот же media_id → тот же StorageKey. Победитель уже
	// закоммитил строку; проигравший не должен сносить общий объект.
	params := ParamsFingerprint("image/png", false, false, nil)
	owner := ownerID()
	mediaID := uuid.New()
	key, err := storage.BuildKey(owner, mediaID, storage.VariantOriginal, "image/png", "a.png")
	require.NoError(t, err)

	repoStub := &raceAfterPutRepo{
		conflict: repo.ErrConcurrentConflict,
		winner: &repo.Media{
			ID:                mediaID,
			OwnerID:           owner,
			IdempotencyKey:    "k6",
			BodyFingerprint:   "bf-6",
			ParamsFingerprint: params,
			Status:            repo.MediaStatusStored,
			StorageKey:        key,
		},
	}
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	res, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           owner,
		MediaID:           mediaID,
		IdempotencyKey:    "k6",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-6",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("data")),
	})
	require.NoError(t, err)
	require.True(t, res.Replay)
	require.Equal(t, mediaID, res.Media.ID)
	require.Equal(t, 1, sto.puts)
	require.Equal(t, 0, sto.deletes, "must not delete winner's object at shared key")
	require.Contains(t, sto.objects, key)
}

func TestPersistUpload_raceAfterPutDifferentKeyCompensates(t *testing.T) {
	params := ParamsFingerprint("image/png", false, false, nil)
	winnerID := uuid.New()
	repoStub := &raceAfterPutRepo{
		conflict: repo.ErrConcurrentConflict,
		winner: &repo.Media{
			ID:                winnerID,
			OwnerID:           ownerID(),
			IdempotencyKey:    "k6b",
			BodyFingerprint:   "bf-6b",
			ParamsFingerprint: params,
			Status:            repo.MediaStatusStored,
			StorageKey:        "winner/other-media/original.png",
		},
	}
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	res, err := svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           ownerID(),
		MediaID:           uuid.New(),
		IdempotencyKey:    "k6b",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-6b",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("data")),
	})
	require.NoError(t, err)
	require.True(t, res.Replay)
	require.Equal(t, winnerID, res.Media.ID)
	require.Equal(t, 1, sto.puts)
	require.Equal(t, 1, sto.deletes)
	require.Empty(t, sto.objects)
}

func TestPersistUpload_idConflictSameKeyDoesNotDelete(t *testing.T) {
	params := ParamsFingerprint("image/png", false, false, nil)
	owner := ownerID()
	mediaID := uuid.New()
	key, err := storage.BuildKey(owner, mediaID, storage.VariantOriginal, "image/png", "a.png")
	require.NoError(t, err)

	repoStub := &raceAfterPutRepo{
		conflict: repo.ErrIDConflict,
		winner: &repo.Media{
			ID:                mediaID,
			OwnerID:           owner,
			IdempotencyKey:    "other-key",
			BodyFingerprint:   "bf-other",
			ParamsFingerprint: params,
			Status:            repo.MediaStatusStored,
			StorageKey:        key,
		},
	}
	sto := newCountingStorage()
	svc := NewService(repoStub, &svcStubDerivRepo{}, sto, time.Minute, svcTestLogger())

	_, err = svc.PersistUpload(context.Background(), PersistUploadInput{
		OwnerID:           owner,
		MediaID:           mediaID,
		IdempotencyKey:    "k-id-conflict",
		Filename:          "a.png",
		Mime:              "image/png",
		Kind:              repo.MediaKindImage,
		SizeBytes:         4,
		BodyFingerprint:   "bf-new",
		ParamsFingerprint: params,
		Reader:            bytes.NewReader([]byte("data")),
	})
	require.ErrorIs(t, err, ErrAlreadyExists)
	require.Equal(t, 1, sto.puts)
	require.Equal(t, 0, sto.deletes)
	require.Contains(t, sto.objects, key)
}

// raceAfterPutRepo: lookup miss until InsertWithJobs races; then returns winner.
type raceAfterPutRepo struct {
	winner      *repo.Media
	conflict    error
	afterInsert bool
}

func (r *raceAfterPutRepo) GetByID(ctx context.Context, id uuid.UUID) (*repo.Media, error) {
	if !r.afterInsert || r.winner.ID != id {
		return nil, repo.ErrNotFound
	}
	cp := *r.winner
	return &cp, nil
}

func (r *raceAfterPutRepo) GetByOwnerIdempotency(ctx context.Context, ownerID uuid.UUID, idempotencyKey string) (*repo.Media, error) {
	if !r.afterInsert {
		return nil, repo.ErrNotFound
	}
	cp := *r.winner
	return &cp, nil
}

func (r *raceAfterPutRepo) InsertWithJobs(ctx context.Context, m repo.Media, jobTypes []string) (*repo.Media, error) {
	r.afterInsert = true
	if r.conflict != nil {
		return nil, r.conflict
	}
	return nil, repo.ErrConcurrentConflict
}

func (r *raceAfterPutRepo) ListDeleting(ctx context.Context, olderThan time.Time, limit int) ([]*repo.Media, error) {
	return nil, nil
}
func (r *raceAfterPutRepo) HardDelete(ctx context.Context, id uuid.UUID) error { return nil }
func (r *raceAfterPutRepo) ExistsBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	return nil, nil
}

func (s *raceAfterPutRepo) CreateAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) error {
	return nil
}

func (s *raceAfterPutRepo) DeleteAttachment(ctx context.Context, mediaID, ownerID uuid.UUID) (usagesRemaining int, err error) {
	return 0, nil
}
