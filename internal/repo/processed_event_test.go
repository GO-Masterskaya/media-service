package repo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ProcessedEventSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	pool      *pgxpool.Pool
	repo      ProcessedEventRepo
}

func TestProcessedEventIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(ProcessedEventSuite))
}

func (s *ProcessedEventSuite) SetupSuite() {
	s.ctx = context.Background()

	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		if !dockerAvailable() {
			s.T().Skip("Docker not available; set TEST_POSTGRES_DSN to run integration tests with external Postgres")
		}
		dsn = s.startPostgres()
	}

	require.NoError(s.T(), RunMigrations(dsn))

	var err error
	s.pool, err = pgxpool.New(s.ctx, dsn)
	require.NoError(s.T(), err)

	s.repo = NewPgProcessedEventRepo(s.pool)
}

func (s *ProcessedEventSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.container != nil {
		require.NoError(s.T(), s.container.Terminate(s.ctx))
	}
}

func (s *ProcessedEventSuite) SetupTest() {
	_, err := s.pool.Exec(s.ctx, `TRUNCATE processed_events`)
	require.NoError(s.T(), err)
}

// startPostgres — вынесено в метод suite, чтобы использовать s.ctx и s.T().
func (s *ProcessedEventSuite) startPostgres() string {
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
	s.container, err = testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(s.T(), err)

	host, err := s.container.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.container.MappedPort(s.ctx, "5432")
	require.NoError(s.T(), err)

	return fmt.Sprintf("postgres://media:media@%s:%s/media?sslmode=disable", host, port.Port())
}

// TestFreshClaim - новый event_id: claim успешен, статус processing.
func (s *ProcessedEventSuite) TestFreshClaim() {
	t := s.T()
	eventID := uuid.New()

	ev, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, EventStatusProcessing, ev.Status)
	require.Equal(t, "worker-a", ev.Owner)
}

// TestConcurrentClaim - второй consumer того же event_id с живым lease не
// выполняет side effect параллельно.
func (s *ProcessedEventSuite) TestConcurrentClaim() {
	t := s.T()
	eventID := uuid.New()

	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)

	_, claimed, err = s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.ErrorIs(t, err, ErrClaimHeld)
	require.False(t, claimed)
}

// TestReplayAfterDone - повтор done event возвращает сохранённый result и
// не запускает side effect повторно.
func (s *ProcessedEventSuite) TestReplayAfterDone() {
	t := s.T()
	eventID := uuid.New()

	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, s.repo.MarkDone(s.ctx, eventID, "worker-a", []byte(`{"media_id":"abc"}`)))

	ev, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, EventStatusDone, ev.Status)
	require.JSONEq(t, `{"media_id":"abc"}`, string(ev.Result))
}

// TestFingerprintConflict - тот же event_id, другой payload fingerprint -
// конфликт, событие не исполняется.
func (s *ProcessedEventSuite) TestFingerprintConflict() {
	t := s.T()
	eventID := uuid.New()

	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, s.repo.MarkDone(s.ctx, eventID, "worker-a", []byte(`{}`)))

	_, claimed, err = s.repo.Claim(s.ctx, eventID, "fp-DIFFERENT", "worker-b", time.Minute)
	require.ErrorIs(t, err, ErrFingerprintConflict)
	require.False(t, claimed)
}

// TestFingerprintConflictOnExpiredLease - регрессия на дыру в идемпотентности:
// событие с тем же event_id, но другим payload, НЕ должно перехватывать
// протухший processing-claim. Без сверки fingerprint в WHERE апсерта такой
// claim проходил и side effect выполнялся для чужого тела.
func (s *ProcessedEventSuite) TestFingerprintConflictOnExpiredLease() {
	t := s.T()
	eventID := uuid.New()

	// worker-a берёт claim с коротким lease и «умирает», не финализировав.
	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", 50*time.Millisecond)
	require.NoError(t, err)
	require.True(t, claimed)

	time.Sleep(100 * time.Millisecond) // lease протух

	// Тот же event_id, но payload другой - перехват запрещён.
	_, claimed, err = s.repo.Claim(s.ctx, eventID, "fp-DIFFERENT", "worker-b", time.Minute)
	require.ErrorIs(t, err, ErrFingerprintConflict)
	require.False(t, claimed)

	// Владелец не сменился: строка осталась за worker-a со своим fingerprint.
	ev, err := s.repo.(*PgProcessedEventRepo).getByID(s.ctx, eventID)
	require.NoError(t, err)
	require.Equal(t, "fp-1", ev.Fingerprint)
	require.Equal(t, "worker-a", ev.Owner)

	// А тот же payload протухший claim забрать по-прежнему может.
	ev, claimed, err = s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, "worker-b", ev.Owner)
}

// TestStaleClaimReclaimed - просроченный processing claim можно забрать;
// живой - нельзя (перепроверяем оба конца).
func (s *ProcessedEventSuite) TestStaleClaimReclaimed() {
	t := s.T()
	eventID := uuid.New()

	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", 50*time.Millisecond)
	require.NoError(t, err)
	require.True(t, claimed)

	// Живой lease - вторая попытка отбивается.
	_, claimed, err = s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.ErrorIs(t, err, ErrClaimHeld)
	require.False(t, claimed)

	time.Sleep(100 * time.Millisecond)

	ev, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, "worker-b", ev.Owner)
}

// TestMarkDoneClaimLost - если lease был перехвачен, финализация от старого
// владельца не проходит молча.
func (s *ProcessedEventSuite) TestMarkDoneClaimLost() {
	t := s.T()
	eventID := uuid.New()

	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", 50*time.Millisecond)
	require.NoError(t, err)
	require.True(t, claimed)

	time.Sleep(100 * time.Millisecond)

	_, claimed, err = s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)

	err = s.repo.MarkDone(s.ctx, eventID, "worker-a", []byte(`{}`))
	require.ErrorIs(t, err, ErrClaimLost)
}

// TestMarkDLQ - DLQ-финализация сохраняет причину и тоже видна при реплее.
func (s *ProcessedEventSuite) TestMarkDLQ() {
	t := s.T()
	eventID := uuid.New()

	_, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, s.repo.MarkDLQ(s.ctx, eventID, "worker-a", "invalid schema"))

	ev, claimed, err := s.repo.Claim(s.ctx, eventID, "fp-1", "worker-b", time.Minute)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, EventStatusDLQ, ev.Status)
	require.JSONEq(t, `{"reason":"invalid schema"}`, string(ev.Result))
}

// TestDeleteTerminalOlderThan - retention чистит только терминальные записи
// старше окна; processing не трогается никогда.
func (s *ProcessedEventSuite) TestDeleteTerminalOlderThan() {
	t := s.T()

	oldDone := uuid.New()
	oldDLQ := uuid.New()
	oldProcessing := uuid.New()
	freshDone := uuid.New()

	// Три записи, которые «состарим», и одна свежая.
	for id, fin := range map[uuid.UUID]string{
		oldDone:       "done",
		oldDLQ:        "dlq",
		oldProcessing: "",
		freshDone:     "done",
	} {
		_, claimed, err := s.repo.Claim(s.ctx, id, "fp-1", "worker-a", time.Minute)
		require.NoError(t, err)
		require.True(t, claimed)

		switch fin {
		case "done":
			require.NoError(t, s.repo.MarkDone(s.ctx, id, "worker-a", []byte(`{}`)))
		case "dlq":
			require.NoError(t, s.repo.MarkDLQ(s.ctx, id, "worker-a", "boom"))
		}
	}

	// Сдвигаем created_at в прошлое для трёх записей.
	_, err := s.pool.Exec(s.ctx,
		`UPDATE processed_events SET created_at = now() - interval '30 days'
		 WHERE event_id IN ($1, $2, $3)`,
		oldDone, oldDLQ, oldProcessing)
	require.NoError(t, err)

	deleted, err := s.repo.DeleteTerminalOlderThan(s.ctx, time.Now().Add(-24*time.Hour), 100)
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted, "должны удалиться только старые done и dlq")

	repoImpl := s.repo.(*PgProcessedEventRepo)

	// Старые терминальные - удалены.
	_, err = repoImpl.getByID(s.ctx, oldDone)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = repoImpl.getByID(s.ctx, oldDLQ)
	require.ErrorIs(t, err, ErrNotFound)

	// Старый processing - на месте: он ещё нужен для recovery.
	_, err = repoImpl.getByID(s.ctx, oldProcessing)
	require.NoError(t, err)

	// Свежий done - тоже на месте: он вне окна retention.
	_, err = repoImpl.getByID(s.ctx, freshDone)
	require.NoError(t, err)
}
