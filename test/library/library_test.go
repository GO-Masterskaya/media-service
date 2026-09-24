// Package library_test проверяет библиотеку mediaservice снаружи репозитория.
//
// Это отдельный Go-модуль, и в этом весь смысл. Тесты в pkg/mediaservice лежат
// внутри того же модуля, поэтому компилятор разрешает им импортировать
// internal/media и internal/repo - и errors_test.go этим намеренно пользуется.
// Настоящий потребитель так не может: импорт internal разрешён только пакетам,
// чей путь начинается с корня репозитория.
//
// Отсюда свойство, которого нельзя получить иначе: если публичного API окажется
// недостаточно для полного цикла работы, этот модуль просто не скомпилируется.
package library_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/GO-Masterskaya/media-service/pkg/mediaservice"
)

const (
	testBucket = "media-library-test"
	minioUser  = "minioadmin"
	minioPass  = "minioadmin"
)

// LibrarySuite поднимает Postgres и MinIO и держит их на все тесты модуля.
// gRPC-сервер не поднимается: библиотека работает прямыми вызовами.
type LibrarySuite struct {
	suite.Suite

	ctx context.Context

	pgContainer    testcontainers.Container
	minioContainer testcontainers.Container

	dsn           string
	minioEndpoint string

	client *mediaservice.Client
}

func TestLibrary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if !dockerAvailable() {
		t.Skip("docker is not available, skipping integration test")
	}
	suite.Run(t, new(LibrarySuite))
}

func (s *LibrarySuite) SetupSuite() {
	s.ctx = context.Background()

	s.dsn = s.startPostgres()
	s.minioEndpoint = s.startMinIO()

	// Бакет создаём сами: в контракте Deps.Bucket записано, что библиотека
	// бакеты не создаёт. Это единственное, что приходится делать в обход неё.
	s.createBucket()

	// Схему накатывает сама библиотека, опцией. Отдельный вызов Migrate
	// проверен в pkg/mediaservice; здесь интересен путь потребителя,
	// который просто просит клиента разобраться со схемой самому.
	client, err := mediaservice.New(s.ctx, mediaservice.Config{
		PostgresDSN: s.dsn,
		MinIO: mediaservice.MinIOConfig{
			Endpoint:  s.minioEndpoint,
			AccessKey: minioUser,
			SecretKey: minioPass,
			Bucket:    testBucket,
		},
	}, mediaservice.WithAutoMigrate())
	require.NoError(s.T(), err)
	s.client = client
}

func (s *LibrarySuite) TearDownSuite() {
	if s.client != nil {
		require.NoError(s.T(), s.client.Close())
	}
	if s.pgContainer != nil {
		require.NoError(s.T(), s.pgContainer.Terminate(s.ctx))
	}
	if s.minioContainer != nil {
		require.NoError(s.T(), s.minioContainer.Terminate(s.ctx))
	}
}

// TestFullCycle - сценарий из критерия приёмки одним тестом: загрузка,
// чтение метаданных, выборка списком, временная ссылка, чтение потоком,
// удаление.
//
// Проверяется не только то, что каждый шаг отвечает без ошибки, но и то,
// что данные доезжают неискажёнными: содержимое сравнивается побайтно.
func (s *LibrarySuite) TestFullCycle() {
	t := s.T()
	s.requireFFprobe()

	owner := uuid.New()
	body := wavBytes(t, 64*1024)

	res, err := s.client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		Filename:       "sample.wav",
		MIMEType:       "audio/wav",
		ExpectedSize:   uint64(len(body)),
		IdempotencyKey: uuid.NewString(),
	}, bytes.NewReader(body))
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, res.ID)

	// Без WithProcessing объект остаётся в Stored: задач никто не создаёт.
	require.Equal(t, mediaservice.StatusStored, res.Status)

	got, err := s.client.GetMedia(s.ctx, owner, res.ID)
	require.NoError(t, err)
	require.Equal(t, res.ID, got.ID)
	require.Equal(t, owner, got.OwnerID)
	require.Equal(t, mediaservice.KindAudio, got.Kind)
	require.Equal(t, "audio/wav", got.MIMEType)
	require.Equal(t, uint64(len(body)), got.SizeBytes)
	require.Equal(t, "sample.wav", got.Filename)
	require.Empty(t, got.Derivatives)

	list, err := s.client.ListByOwner(s.ctx, mediaservice.ListParams{OwnerID: owner})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Equal(t, res.ID, list.Items[0].ID)
	require.Empty(t, list.NextPageToken)

	link, err := s.client.GetDownloadURL(s.ctx, owner, res.ID, mediaservice.VariantOriginal)
	require.NoError(t, err)
	require.NotEmpty(t, link.URL)
	require.True(t, link.ExpiresAt.After(time.Now()),
		"ссылка выдана уже просроченной")

	rc, err := s.client.DownloadStream(s.ctx, owner, res.ID, mediaservice.VariantOriginal)
	require.NoError(t, err)
	downloaded, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, body, downloaded, "содержимое изменилось при передаче")

	require.NoError(t, s.client.Delete(s.ctx, owner, res.ID))

	_, err = s.client.GetMedia(s.ctx, owner, res.ID)
	require.ErrorIs(t, err, mediaservice.ErrNotFound,
		"объект доступен после удаления")
}

// TestUnknownMediaIsNotFound закрепляет, что типизированные ошибки видны
// снаружи модуля: потребитель отличает «нет такого» от «всё сломалось»
// через errors.Is, не разбирая текст.
func (s *LibrarySuite) TestUnknownMediaIsNotFound() {
	t := s.T()

	_, err := s.client.GetMedia(s.ctx, uuid.New(), uuid.New())
	require.ErrorIs(t, err, mediaservice.ErrNotFound)
	require.False(t, errors.Is(err, mediaservice.ErrInvalidArgument))
}

// requireFFprobe пропускает тест, если ffprobe не установлен.
//
// Библиотека определяет тип файла анализом содержимого, внешним бинарником.
// Без него Upload не работает в принципе, поэтому тест не падает, а честно
// пропускается: в CI ffmpeg ставится, на машине разработчика может не стоять.
func (s *LibrarySuite) requireFFprobe() {
	s.T().Helper()
	if _, err := exec.LookPath("ffprobe"); err != nil {
		s.T().Skip("ffprobe not installed, skipping upload test")
	}
}

func dockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "info").Run() == nil
}

func (s *LibrarySuite) startPostgres() string {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "media",
			"POSTGRES_PASSWORD": "media",
			"POSTGRES_DB":       "media",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}

	var err error
	s.pgContainer, err = testcontainers.GenericContainer(s.ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(s.T(), err)

	host, err := s.pgContainer.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.pgContainer.MappedPort(s.ctx, "5432")
	require.NoError(s.T(), err)

	return fmt.Sprintf("postgres://media:media@%s:%s/media?sslmode=disable",
		host, port.Port())
}

func (s *LibrarySuite) startMinIO() string {
	req := testcontainers.ContainerRequest{
		// MinIO CE больше не отдаётся с Docker Hub/Quay анонимно; Chainguard — публичный rebuild.
		Image:        "cgr.dev/chainguard/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Cmd:          []string{"server", "/data"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     minioUser,
			"MINIO_ROOT_PASSWORD": minioPass,
		},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000"),
	}

	var err error
	s.minioContainer, err = testcontainers.GenericContainer(s.ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(s.T(), err)

	host, err := s.minioContainer.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.minioContainer.MappedPort(s.ctx, "9000")
	require.NoError(s.T(), err)

	return fmt.Sprintf("%s:%s", host, port.Port())
}

func (s *LibrarySuite) createBucket() {
	mc, err := minio.New(s.minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPass, ""),
		Secure: false,
	})
	require.NoError(s.T(), err)
	require.NoError(s.T(), mc.MakeBucket(s.ctx, testBucket, minio.MakeBucketOptions{}))
}

// wavBytes собирает валидный WAV заданного размера: 44-байтный заголовок
// и шум вместо сэмплов.
//
// Почему WAV, а не картинка. Библиотека проверяет содержимое через ffprobe,
// поэтому случайные байты не пройдут - Upload ответит ErrInvalidArgument.
// WAV удобен тем, что несжатый: размер задаётся точно, генерация стоит один
// проход по срезу, и нужный объём получается без кодирования.
//
// Шум, а не тишина, по той же причине - чтобы размер файла нельзя было
// случайно уменьшить сжатием где-то по дороге.
func wavBytes(t require.TestingT, dataSize int) []byte {
	var buf bytes.Buffer
	writeWAVHeader(&buf, dataSize)

	data := make([]byte, dataSize)
	// Детерминированный источник: тест должен падать одинаково.
	rnd := rand.New(rand.NewSource(1))
	_, err := rnd.Read(data)
	require.NoError(t, err)
	buf.Write(data)

	return buf.Bytes()
}

// writeWAVFile пишет такой же WAV сразу на диск, не собирая его в памяти.
//
// Для теста на потоковость это принципиально: если материал держать в срезе,
// пик памяти вырастет от самого теста, и измерять станет нечего.
func writeWAVFile(t require.TestingT, path string, dataSize int) string {
	f, err := os.Create(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	writeWAVHeader(f, dataSize)

	const chunk = 256 * 1024
	buf := make([]byte, chunk)
	rnd := rand.New(rand.NewSource(1))

	for written := 0; written < dataSize; {
		n := chunk
		if rest := dataSize - written; rest < n {
			n = rest
		}
		_, err := rnd.Read(buf[:n])
		require.NoError(t, err)
		_, err = f.Write(buf[:n])
		require.NoError(t, err)
		written += n
	}

	require.NoError(t, f.Sync())
	return path
}

// writeWAVHeader пишет канонический 44-байтный заголовок RIFF/WAVE:
// PCM, 16 бит, моно, 44100 Гц.
func writeWAVHeader(w io.Writer, dataSize int) {
	const (
		sampleRate    = 44100
		channels      = 1
		bitsPerSample = 16
	)
	byteRate := sampleRate * channels * bitsPerSample / 8
	blockAlign := channels * bitsPerSample / 8

	le := binary.LittleEndian
	hdr := make([]byte, 44)
	copy(hdr[0:4], "RIFF")
	le.PutUint32(hdr[4:8], uint32(36+dataSize))
	copy(hdr[8:12], "WAVE")
	copy(hdr[12:16], "fmt ")
	le.PutUint32(hdr[16:20], 16) // размер fmt-чанка
	le.PutUint16(hdr[20:22], 1)  // PCM
	le.PutUint16(hdr[22:24], channels)
	le.PutUint32(hdr[24:28], sampleRate)
	le.PutUint32(hdr[28:32], uint32(byteRate))
	le.PutUint16(hdr[32:34], uint16(blockAlign))
	le.PutUint16(hdr[34:36], bitsPerSample)
	copy(hdr[36:40], "data")
	le.PutUint32(hdr[40:44], uint32(dataSize))

	_, _ = w.Write(hdr)
}

// sha256File считает хеш файла потоком: материал теста в память не поднимается.
func sha256File(t require.TestingT, path string) string {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

// tempWAV кладёт WAV нужного размера во временный каталог теста.
func tempWAV(t *testing.T, name string, dataSize int) string {
	t.Helper()
	return writeWAVFile(t, filepath.Join(t.TempDir(), name), dataSize)
}
