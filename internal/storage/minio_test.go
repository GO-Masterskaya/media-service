package storage

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type MinIOSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	storage   Interface
	client    *minio.Client
	bucket    string
	endpoint  string
}

func TestMinIOIntegration(t *testing.T) {
	suite.Run(t, new(MinIOSuite))
}

func (s *MinIOSuite) SetupSuite() {
	if _, err := exec.LookPath("docker"); err != nil {
		s.T().Skip("docker not found in PATH, skipping integration tests")
	}
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		s.T().Skip("docker daemon not available, skipping integration tests")
	}

	s.ctx = context.Background()
	s.bucket = "media"
	s.ctx = context.Background()
	s.bucket = "media"

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
		ExposedPorts: []string{"9000/tcp"},
		Cmd:          []string{"server", "/data"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     "minioadmin",
			"MINIO_ROOT_PASSWORD": "minioadmin",
		},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000"),
	}

	var err error
	s.container, err = testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(s.T(), err)

	host, err := s.container.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.container.MappedPort(s.ctx, "9000")
	require.NoError(s.T(), err)

	s.endpoint = host + ":" + port.Port()

	s.client, err = minio.New(s.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	require.NoError(s.T(), err)

	err = s.client.MakeBucket(s.ctx, s.bucket, minio.MakeBucketOptions{})
	require.NoError(s.T(), err)

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	s.storage, err = NewMinIO(MinIOConfig{
		Endpoint:  s.endpoint,
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Bucket:    s.bucket,
		UseSSL:    false,
	}, log)
	require.NoError(s.T(), err)
}

func (s *MinIOSuite) TearDownSuite() {
	if s.storage != nil {
		require.NoError(s.T(), s.storage.Close())
	}
	if s.container != nil {
		require.NoError(s.T(), s.container.Terminate(s.ctx))
	}
}

func (s *MinIOSuite) SetupTest() {
	// Очистка перед каждым тестом — удаляем всё в бакете.
	for obj := range s.client.ListObjects(s.ctx, s.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		require.NoError(s.T(), s.client.RemoveObject(s.ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}))
	}
}

// TestPutAndGet — базовый streaming put/get.
func (s *MinIOSuite) TestPutAndGet() {
	t := s.T()
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	media := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	key, err := BuildKey(owner, media, VariantOriginal, "image/png", "test.png")
	require.NoError(t, err)

	data := []byte("fake-png-body")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "image/png"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestPutStreamingWithoutSize — put с неизвестным размером (не буферизуем в память).
func (s *MinIOSuite) TestPutStreamingWithoutSize() {
	t := s.T()
	owner := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	media := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	key, err := BuildKey(owner, media, VariantOriginal, "video/mp4", "movie.mp4")
	require.NoError(t, err)

	data := []byte("streaming-video-body")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), -1, "video/mp4"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestGetObject_NotFound — ошибка на несуществующем ключе.
func (s *MinIOSuite) TestGetObject_NotFound() {
	t := s.T()
	owner := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	media := uuid.MustParse("00000000-0000-0000-0000-000000000001") // ← не Nil

	key, err := BuildKey(owner, media, VariantOriginal, "image/png", "x.png")
	require.NoError(t, err)

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	_, err = io.Copy(io.Discard, rc)
	require.Error(t, err)
}

// TestPresign — TTL работает, URL реально качается, после истечения — 403.
func (s *MinIOSuite) TestPresign() {
	t := s.T()
	owner := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	media := uuid.MustParse("66666666-6666-6666-6666-666666666666")

	key, err := BuildKey(owner, media, VariantOriginal, "audio/mpeg", "song.mp3")
	require.NoError(t, err)

	data := []byte("fake-audio")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "audio/mpeg"))

	ttl := 2 * time.Second
	ps, err := s.storage.PresignGetObject(s.ctx, key, ttl)
	require.NoError(t, err)
	assert.NotEmpty(t, ps.URL)
	assert.WithinDuration(t, time.Now().UTC().Add(ttl), ps.ExpiresAt, 2*time.Second)

	// Ссылка работает.
	resp, err := http.Get(ps.URL)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, data, body)

	// Ждём протухания.
	time.Sleep(3 * time.Second)
	resp2, err := http.Get(ps.URL)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp2.Body.Close()) }()
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode)
}

// TestDeleteObjectIdempotent — удаление и повтор на отсутствующем = OK.
func (s *MinIOSuite) TestDeleteObjectIdempotent() {
	t := s.T()
	owner := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	media := uuid.MustParse("88888888-8888-8888-8888-888888888888")

	key, err := BuildKey(owner, media, VariantOriginal, "image/jpeg", "photo.jpg")
	require.NoError(t, err)

	data := []byte("photo-bytes")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "image/jpeg"))

	require.NoError(t, s.storage.DeleteObject(s.ctx, key))

	// Идемпотентность.
	require.NoError(t, s.storage.DeleteObject(s.ctx, key))
}

// TestDeletePrefix — удаляет все объекты owner/media (оригинал + производные).
func (s *MinIOSuite) TestDeletePrefix() {
	t := s.T()
	owner := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	media := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	variants := []Variant{VariantOriginal, VariantThumb, VariantR720}
	var keys []string
	for _, v := range variants {
		key, err := BuildKey(owner, media, v, "video/mp4", "movie.mp4")
		require.NoError(t, err)
		keys = append(keys, key)
		require.NoError(t, s.storage.PutObject(s.ctx, key, strings.NewReader("data-"+string(v)), 10, "video/mp4"))
	}

	prefix := path.Dir(keys[0]) + "/"
	require.NoError(t, s.storage.DeletePrefix(s.ctx, prefix))

	for _, k := range keys {
		rc, err := s.storage.GetObject(s.ctx, k)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, rc)
		require.NoError(t, rc.Close())
		require.Error(t, err, "key %s should be deleted", k)
	}
}

// TestDeletePrefix_EmptyPrefixRejected — защита от footgun.
func (s *MinIOSuite) TestDeletePrefix_EmptyPrefixRejected() {
	t := s.T()
	err := s.storage.DeletePrefix(s.ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty prefix")
}

// TestKeySanitization — спецсимволы и path traversal в filename не влияют на ключ.
func (s *MinIOSuite) TestKeySanitization() {
	t := s.T()
	owner := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")
	media := uuid.MustParse("dddddddd-dddd-dddd-dddd-dddddddddddd")

	key, err := BuildKey(owner, media, VariantOriginal, "image/png", "../../../etc/passwd")
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(key, owner.String()+"/"+media.String()+"/"))
	assert.False(t, strings.Contains(key, ".."))
	assert.False(t, strings.Contains(key, "/etc/passwd"))

	data := []byte("safe")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "image/png"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestPutDoesNotBufferInMemory — косвенная проверка: reader без предварительного чтения в слайс.
func (s *MinIOSuite) TestPutDoesNotBufferInMemory() {
	t := s.T()
	owner := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	media := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")

	key, err := BuildKey(owner, media, VariantOriginal, "application/octet-stream", "big.bin")
	require.NoError(t, err)

	// Генератор 1 МБ без хранения в памяти.
	r := &byteGenerator{limit: 1024 * 1024}
	require.NoError(t, s.storage.PutObject(s.ctx, key, r, -1, "application/octet-stream"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	n, err := io.Copy(io.Discard, rc)
	require.NoError(t, err)
	assert.Equal(t, int64(1024*1024), n)
}

// byteGenerator — io.Reader, который генерирует N байт на лету.
type byteGenerator struct {
	limit int64
	sent  int64
}

func (g *byteGenerator) Read(p []byte) (int, error) {
	if g.sent >= g.limit {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > g.limit-g.sent {
		n = int(g.limit - g.sent)
	}
	for i := range n {
		p[i] = byte(g.sent % 256)
		g.sent++
	}
	return n, nil
}

func (s *MinIOSuite) TestForEachObject_UploadStartedAt() {
	t := s.T()
	owner := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	media := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")

	key, err := BuildKey(owner, media, VariantOriginal, "image/png", "test.png")
	require.NoError(t, err)

	data := []byte("test-body")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "image/png"))

	var found bool
	err = s.storage.ForEachObject(s.ctx, "", func(obj ObjectInfo) error {
		if obj.Key == key {
			found = true
			assert.False(t, obj.UploadStartedAt.IsZero(), "UploadStartedAt should be set from user-metadata")
			assert.WithinDuration(t, time.Now(), obj.UploadStartedAt, 10*time.Second)
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, found, "object not found in ForEachObject")
}
