package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
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
	s.ctx = context.Background()
	s.bucket = "media"

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
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

	s.storage, err = NewMinIO(MinIOConfig{
		Endpoint:  s.endpoint,
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Bucket:    s.bucket,
		UseSSL:    false,
	})
	require.NoError(s.T(), err)
}

func (s *MinIOSuite) TearDownSuite() {
	if s.container != nil {
		_ = s.container.Terminate(s.ctx)
	}
}

func (s *MinIOSuite) cleanPrefix(prefix string) {
	_ = s.storage.DeletePrefix(s.ctx, prefix)
}

// TestPutAndGet — базовый streaming put/get.
func (s *MinIOSuite) TestPutAndGet() {
	t := s.T()
	owner := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	media := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	key, err := BuildKey(owner, media, VariantOriginal, "image/png", "test.png")
	require.NoError(t, err)
	s.cleanPrefix(owner.String() + "/")

	data := []byte("fake-png-body")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "image/png"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, rc.Close())
	}()

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
	s.cleanPrefix(owner.String() + "/")

	data := []byte("streaming-video-body")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), -1, "video/mp4"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rc.Close())
	}()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestPresign — TTL работает, URL реально качается, после истечения — 403.
func (s *MinIOSuite) TestPresign() {
	t := s.T()
	owner := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	media := uuid.MustParse("66666666-6666-6666-6666-666666666666")

	key, err := BuildKey(owner, media, VariantOriginal, "audio/mpeg", "song.mp3")
	require.NoError(t, err)
	s.cleanPrefix(owner.String() + "/")

	data := []byte("fake-audio")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "audio/mpeg"))

	ttl := 5 * time.Second
	ps, err := s.storage.PresignGetObject(s.ctx, key, ttl)
	require.NoError(t, err)
	assert.NotEmpty(t, ps.URL)
	assert.WithinDuration(t, time.Now().Add(ttl), ps.ExpiresAt, 2*time.Second)

	// Ссылка работает.
	resp, err := http.Get(ps.URL)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, data, body)

	// Ждём протухания.
	time.Sleep(6 * time.Second)
	resp2, err := http.Get(ps.URL)
	require.NoError(t, err)
	defer func() {
		s.Require().NoError(resp2.Body.Close())
	}()
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode) // MinIO отдаёт 403 на expired presign
}

// TestDeleteObjectIdempotent — удаление и повтор на отсутствующем = OK.
func (s *MinIOSuite) TestDeleteObjectIdempotent() {
	t := s.T()
	owner := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	media := uuid.MustParse("88888888-8888-8888-8888-888888888888")

	key, err := BuildKey(owner, media, VariantOriginal, "image/jpeg", "photo.jpg")
	require.NoError(t, err)
	s.cleanPrefix(owner.String() + "/")

	data := []byte("photo-bytes")
	require.NoError(t, s.storage.PutObject(s.ctx, key, bytes.NewReader(data), int64(len(data)), "image/jpeg"))

	require.NoError(t, s.storage.DeleteObject(s.ctx, key))
	_, err = s.storage.GetObject(s.ctx, key)
	require.Error(t, err) // уже удалён

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

	prefix := owner.String() + "/" + media.String() + "/"
	require.NoError(t, s.storage.DeletePrefix(s.ctx, prefix))

	for _, k := range keys {
		_, err := s.storage.GetObject(s.ctx, k)
		require.Error(t, err, "key %s should be deleted", k)
	}
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
	defer func() {
		require.NoError(t, rc.Close())
	}()

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
	s.cleanPrefix(owner.String() + "/")

	// Генератор 1 МБ без хранения в памяти.
	r := &byteGenerator{limit: 1024 * 1024}
	require.NoError(t, s.storage.PutObject(s.ctx, key, r, -1, "application/octet-stream"))

	rc, err := s.storage.GetObject(s.ctx, key)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rc.Close())
	}()

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
