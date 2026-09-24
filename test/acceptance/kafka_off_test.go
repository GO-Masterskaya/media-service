package acceptance_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/GO-Masterskaya/media-service/internal/api"
	"github.com/GO-Masterskaya/media-service/internal/api/interceptors"
	"github.com/GO-Masterskaya/media-service/internal/media"
	"github.com/GO-Masterskaya/media-service/internal/metrics"
	"github.com/GO-Masterskaya/media-service/internal/repo"
	"github.com/GO-Masterskaya/media-service/internal/storage"
	"github.com/GO-Masterskaya/media-service/internal/upload"
	mediav1 "github.com/GO-Masterskaya/media-service/proto/media/v1"
)

// NoKafkaSuite — KAFKA_ENABLED=false: без Redpanda/consumer, gRPC upload работает.
type NoKafkaSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	pgContainer    testcontainers.Container
	minioContainer testcontainers.Container

	dsn           string
	minioEndpoint string
	tempDir       string

	pool       *pgxpool.Pool
	grpcServer *grpc.Server
	client     mediav1.MediaServiceClient
	conn       *grpc.ClientConn
	lis        *bufconn.Listener
	ownerID    uuid.UUID
}

func TestAcceptanceNoKafka(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance tests in short mode")
	}
	if !dockerAvailable() {
		t.Skip("docker is not available, skipping acceptance tests")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not available, skipping acceptance tests")
	}
	suite.Run(t, new(NoKafkaSuite))
}

func (s *NoKafkaSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.ownerID = uuid.MustParse("22222222-2222-2222-2222-222222222222")

	s.dsn = s.startPostgres()
	s.minioEndpoint = s.startMinIO()
	s.createBucket()
	require.NoError(s.T(), repo.RunMigrations(s.dsn))

	dir, err := os.MkdirTemp("", "media-acceptance-nokafka-*")
	require.NoError(s.T(), err)
	s.tempDir = dir

	s.startServerNoKafka()
}

func (s *NoKafkaSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.lis != nil {
		_ = s.lis.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}
	termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if s.pgContainer != nil {
		_ = s.pgContainer.Terminate(termCtx)
	}
	if s.minioContainer != nil {
		_ = s.minioContainer.Terminate(termCtx)
	}
}

func (s *NoKafkaSuite) authCtx() context.Context {
	md := metadata.Pairs(
		"authorization", "Bearer "+authToken,
		"x-owner-id", s.ownerID.String(),
	)
	return metadata.NewOutgoingContext(s.ctx, md)
}

func (s *NoKafkaSuite) TestUploadWorksWithoutKafka() {
	t := s.T()

	stream, err := s.client.Upload(s.authCtx())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Init{Init: &mediav1.UploadInit{
			OwnerId:        s.ownerID.String(),
			Filename:       "nokafka.png",
			Mime:           "image/png",
			ExpectedSize:   uint64(len(png64)),
			IdempotencyKey: uuid.NewString(),
		}},
	}))
	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Chunk{Chunk: png64},
	}))
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.NotEmpty(t, resp.MediaId)
	require.Equal(t, mediav1.MediaStatus_STORED, resp.Status)

	m, err := s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{MediaId: resp.MediaId})
	require.NoError(t, err)
	require.Equal(t, mediav1.MediaStatus_STORED, m.Status)
}

func (s *NoKafkaSuite) startServerNoKafka() {
	t := s.T()

	pool, err := repo.NewPool(s.ctx, repo.PoolConfig{
		DSN:            s.dsn,
		ConnectTimeout: 10 * time.Second,
		QueryTimeout:   30 * time.Second,
	})
	require.NoError(t, err)
	s.pool = pool

	sto, err := storage.NewMinIO(storage.MinIOConfig{
		Endpoint:  s.minioEndpoint,
		AccessKey: minioUser,
		SecretKey: minioPass,
		Bucket:    testBucket + "-nokafka",
		UseSSL:    false,
	}, slog.Default())
	require.NoError(t, err)

	mediaRepo := repo.NewPgMediaRepo(pool)
	derivRepo := repo.NewPgDerivativeRepo(pool)
	mediaSvc := media.NewService(mediaRepo, derivRepo, sto, 15*time.Minute, slog.Default())

	reg := prometheus.NewRegistry()
	uploadMetrics := upload.NewMetrics(reg)
	uploadStore, err := upload.New(upload.Config{
		Dir:             filepath.Join(s.tempDir, "uploads"),
		MaxFileSize:     50 << 20,
		ReserveBytes:    1 << 20,
		StaleGrace:      time.Hour,
		CleanupInterval: time.Hour,
	}, uploadMetrics, slog.Default())
	require.NoError(t, err)
	mediaSvc.SetUploadConfig(uploadStore, media.DefaultProber{}, 50<<20,
		[]string{"image/*", "video/*", "audio/*"}, 0)

	validator, err := protovalidate.New()
	require.NoError(t, err)
	grpcMetrics := metrics.NewGRPCMetrics(reg)
	rateLimiter := interceptors.NewRateLimiter(rate.Limit(1000), 1000)
	streamLimiter := interceptors.NewStreamLimiter(32)
	allowlist := interceptors.NewCallerAllowlist(nil)

	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(16<<20),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, allowlist,
			)...,
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamInterceptors(
				true, authToken, validator, grpcMetrics, rateLimiter, streamLimiter, allowlist,
			)...,
		),
	)
	mediav1.RegisterMediaServiceServer(s.grpcServer, api.NewMediaServer(mediaSvc, false, 30*time.Second))

	s.lis = bufconn.Listen(bufSize)
	go func() { _ = s.grpcServer.Serve(s.lis) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return s.lis.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet-nokafka",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	s.conn = conn
	s.client = mediav1.NewMediaServiceClient(conn)
}

func (s *NoKafkaSuite) startPostgres() string {
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
	return fmt.Sprintf("postgres://media:media@%s:%s/media?sslmode=disable", host, port.Port())
}

func (s *NoKafkaSuite) startMinIO() string {
	req := testcontainers.ContainerRequest{
		// MinIO CE больше не отдаётся с Docker Hub/Quay анонимно; Chainguard — публичный rebuild.
		Image:        "cgr.dev/chainguard/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Cmd:          []string{"server", "/data"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     minioUser,
			"MINIO_ROOT_PASSWORD": minioPass,
		},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000").WithStartupTimeout(60 * time.Second),
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

func (s *NoKafkaSuite) createBucket() {
	mc, err := minio.New(s.minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUser, minioPass, ""),
		Secure: false,
	})
	require.NoError(s.T(), err)
	require.NoError(s.T(), mc.MakeBucket(s.ctx, testBucket+"-nokafka", minio.MakeBucketOptions{}))
}
