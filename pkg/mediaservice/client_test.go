package mediaservice_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"mediaservice/pkg/mediaservice"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testBucket = "media-test"
	minioUser  = "minioadmin"
	minioPass  = "minioadmin"

	// appNameOwned помечает соединения пула, созданного самой библиотекой.
	// По этой метке тест считает живые соединения в pg_stat_activity: это
	// единственный способ снаружи убедиться, что Close() закрыл то, чем владел.
	appNameOwned = "mediaservice-owned-pool"
)

// ClientSuite проверяет владение ресурсами: Close() закрывает только
// то, что клиент создал сам, и не трогает переданное извне.
//
// Пакет mediaservice_test, а не mediaservice: тест видит только экспортированный
// API, ровно как встраивающее приложение.
type ClientSuite struct {
	suite.Suite

	ctx context.Context

	pgContainer    testcontainers.Container
	minioContainer testcontainers.Container

	dsn           string
	minioEndpoint string

	// admin - отдельный пул для наблюдения за состоянием сервера.
	// В тестируемых сценариях не участвует и живёт всю сюиту.
	admin *pgxpool.Pool
}

func TestClient(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if !dockerAvailable() {
		t.Skip("docker is not available, skipping integration test")
	}
	suite.Run(t, new(ClientSuite))
}

func (s *ClientSuite) SetupSuite() {
	s.ctx = context.Background()

	s.dsn = s.startPostgres()
	s.minioEndpoint = s.startMinIO()

	// Схему накатываем публичным API библиотеки: заодно убеждаемся,
	// что Migrate работает на чистой базе.
	require.NoError(s.T(), mediaservice.Migrate(s.dsn))

	// Бакет создаём сами: в контракте Deps.Bucket записано,
	// что библиотека бакеты не создаёт.
	s.createBucket()

	var err error
	s.admin, err = pgxpool.New(s.ctx, s.dsn)
	require.NoError(s.T(), err)
}

func (s *ClientSuite) TearDownSuite() {
	if s.admin != nil {
		s.admin.Close()
	}
	if s.pgContainer != nil {
		require.NoError(s.T(), s.pgContainer.Terminate(s.ctx))
	}
	if s.minioContainer != nil {
		require.NoError(s.T(), s.minioContainer.Terminate(s.ctx))
	}
}

// TestNewWithDeps_CloseKeepsExternalResources - основной тест владения.
// Ресурсы, переданные снаружи, обязаны пережить Close().
func (s *ClientSuite) TestNewWithDeps_CloseKeepsExternalResources() {
	t := s.T()

	pool := s.newExternalPool()
	defer pool.Close()
	mc := s.newExternalMinIO()

	client, err := mediaservice.NewWithDeps(s.ctx, mediaservice.Deps{
		Pool:   pool,
		MinIO:  mc,
		Bucket: testBucket,
	})
	require.NoError(t, err)
	require.NoError(t, client.Close())

	// Проверяем не факт закрытия клиента, а состояние ресурсов после него.
	require.NoError(t, pool.Ping(s.ctx),
		"внешний пул закрыт, хотя клиент им не владел")

	exists, err := mc.BucketExists(s.ctx, testBucket)
	require.NoError(t, err,
		"внешний клиент MinIO перестал работать после Close()")
	require.True(t, exists)
}

// TestNew_ClosesOwnedPool - обратный случай: то, что клиент создал сам,
// он обязан закрыть. Изнутри пакета до пула не дотянуться, поэтому смотрим
// со стороны сервера, по метке application_name.
func (s *ClientSuite) TestNew_ClosesOwnedPool() {
	t := s.T()

	client, err := mediaservice.New(s.ctx, mediaservice.Config{
		PostgresDSN: s.dsn + "&application_name=" + appNameOwned,
		Pool:        mediaservice.PoolConfig{MinConns: 2},
		MinIO: mediaservice.MinIOConfig{
			Endpoint:  s.minioEndpoint,
			AccessKey: minioUser,
			SecretKey: minioPass,
			Bucket:    testBucket,
		},
	})
	require.NoError(t, err)

	defer func() { _ = client.Close() }()

	// Пул поднимает соединения фоном, поэтому ждём, а не проверяем сразу.
	// Без этой проверки тест был бы бессмысленным: если соединений
	// не появилось, их исчезновение ничего не доказывает.
	require.Eventually(t, func() bool { return s.countOwnedConns() > 0 },
		10*time.Second, 100*time.Millisecond,
		"пул не открыл ни одного соединения, проверять нечего")

	require.NoError(t, client.Close())

	require.Eventually(t, func() bool { return s.countOwnedConns() == 0 },
		10*time.Second, 100*time.Millisecond,
		"соединения остались открытыми: Close() не закрыл собственный пул")
}

// TestClose_Idempotent - повторное закрытие безвредно.
func (s *ClientSuite) TestClose_Idempotent() {
	t := s.T()

	pool := s.newExternalPool()
	defer pool.Close()

	client, err := mediaservice.NewWithDeps(s.ctx, mediaservice.Deps{
		Pool: pool, MinIO: s.newExternalMinIO(), Bucket: testBucket,
	})
	require.NoError(t, err)

	require.NoError(t, client.Close())
	require.NoError(t, client.Close(), "повторный Close() должен быть безвредным")
}

// TestMethodsAfterClose_ReturnErrClosed - acquire стоит первой строкой
// во всех семи методах, проверяется не только порядок проверок,
// но и что блокировка берётся до всего остального.
func (s *ClientSuite) TestMethodsAfterClose_ReturnErrClosed() {
	t := s.T()

	pool := s.newExternalPool()
	defer pool.Close()

	client, err := mediaservice.NewWithDeps(s.ctx, mediaservice.Deps{
		Pool: pool, MinIO: s.newExternalMinIO(), Bucket: testBucket,
	})
	require.NoError(t, err)
	require.NoError(t, client.Close())

	ownerID, mediaID := uuid.New(), uuid.New()

	cases := []struct {
		name string
		call func() error
	}{
		{"Upload", func() error {
			_, err := client.Upload(s.ctx,
				mediaservice.UploadParams{OwnerID: ownerID},
				strings.NewReader("payload"))
			return err
		}},
		{"GetMedia", func() error {
			_, err := client.GetMedia(s.ctx, ownerID, mediaID)
			return err
		}},
		{"ListByOwner", func() error {
			_, err := client.ListByOwner(s.ctx, mediaservice.ListParams{OwnerID: ownerID})
			return err
		}},
		{"GetDownloadURL", func() error {
			_, err := client.GetDownloadURL(s.ctx, ownerID, mediaID, mediaservice.VariantOriginal)
			return err
		}},
		{"DownloadStream", func() error {
			_, err := client.DownloadStream(s.ctx, ownerID, mediaID, mediaservice.VariantOriginal)
			return err
		}},
		{"Delete", func() error {
			return client.Delete(s.ctx, ownerID, mediaID)
		}},
		{"DeleteByOwner", func() error {
			_, err := client.DeleteByOwner(s.ctx, ownerID)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.call(), mediaservice.ErrClosed)
		})
	}
}

// TestConcurrentCloseAndCalls проверяет, что параллельный Close не выпускает
// наружу ошибку драйвера. acquire держит блокировку на чтение всю операцию,
// поэтому Close берёт её на запись и дожидается завершения начатых вызовов.
//
// Запускать с -race: без него гонка может не проявиться на быстрой машине.
func (s *ClientSuite) TestConcurrentCloseAndCalls() {
	t := s.T()

	pool := s.newExternalPool()
	defer pool.Close()

	client, err := mediaservice.NewWithDeps(s.ctx, mediaservice.Deps{
		Pool: pool, MinIO: s.newExternalMinIO(), Bucket: testBucket,
	})
	require.NoError(t, err)

	const workers = 32

	// start закрывается разом, чтобы все горутины стартовали одновременно
	// и попали в узкое окно между проверкой флага и работой с пулом.
	start := make(chan struct{})
	errs := make([]error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Каждая горутина пишет в свою ячейку: гонки на срезе нет,
			// длина зафиксирована до запуска.
			_, errs[i] = client.GetMedia(s.ctx, uuid.New(), uuid.New())
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_ = client.Close()
	}()

	close(start)
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err,
			"worker %d: GetMedia на случайном UUID обязан вернуть ошибку", i)
		if errors.Is(err, mediaservice.ErrClosed) || errors.Is(err, mediaservice.ErrNotFound) {
			continue
		}
		t.Fatalf("worker %d: получена %v, ожидались ErrClosed или ErrNotFound. "+
			"Скорее всего Close закрыл пул посреди операции", i, err)
	}
}

// --- вспомогательное ---

func dockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "info").Run() == nil
}

func (s *ClientSuite) startPostgres() string {
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

func (s *ClientSuite) startMinIO() string {
	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:RELEASE.2025-09-07T16-13-09Z",
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

func (s *ClientSuite) createBucket() {
	mc := s.newExternalMinIO()
	require.NoError(s.T(), mc.MakeBucket(s.ctx, testBucket, minio.MakeBucketOptions{}))
}

func (s *ClientSuite) newExternalPool() *pgxpool.Pool {
	s.T().Helper()
	pool, err := pgxpool.New(s.ctx, s.dsn)
	require.NoError(s.T(), err)
	return pool
}

func (s *ClientSuite) newExternalMinIO() *minio.Client {
	s.T().Helper()
	mc, err := minio.New(s.minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPass, ""),
		Secure: false,
	})
	require.NoError(s.T(), err)
	return mc
}

func (s *ClientSuite) countOwnedConns() int {
	var n int
	err := s.admin.QueryRow(s.ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`,
		appNameOwned).Scan(&n)
	require.NoError(s.T(), err)
	return n
}
