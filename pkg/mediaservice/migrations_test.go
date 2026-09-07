package mediaservice_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"mediaservice/pkg/mediaservice"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// MigrationsSuite проверяет два способа применить схему из критериев приёмки:
// библиотекой и внешним мигратором встраивающего проекта.
//
// Отдельная сюита, а не методы ClientSuite: здесь не нужен MinIO, зато нужна
// чистая база на каждый тест. Поднимать контейнер под каждый сценарий дорого,
// поэтому базы создаются внутри одного сервера через CREATE DATABASE.
type MigrationsSuite struct {
	suite.Suite

	ctx       context.Context
	container testcontainers.Container

	host string
	port string

	// admin подключён к служебной базе postgres: из неё выполняется
	// CREATE DATABASE. Из базы нельзя создать саму себя, поэтому
	// отдельное подключение обязательно.
	admin *pgxpool.Pool
}

func TestMigrations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if !dockerAvailable() {
		t.Skip("docker is not available, skipping integration test")
	}
	suite.Run(t, new(MigrationsSuite))
}

func (s *MigrationsSuite) SetupSuite() {
	s.ctx = context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "media",
			"POSTGRES_PASSWORD": "media",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}

	var err error
	s.container, err = testcontainers.GenericContainer(s.ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(s.T(), err)

	host, err := s.container.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := s.container.MappedPort(s.ctx, "5432")
	require.NoError(s.T(), err)
	s.host, s.port = host, port.Port()

	s.admin, err = pgxpool.New(s.ctx, s.dsnFor("postgres"))
	require.NoError(s.T(), err)
}

func (s *MigrationsSuite) TearDownSuite() {
	if s.admin != nil {
		s.admin.Close()
	}
	if s.container != nil {
		require.NoError(s.T(), s.container.Terminate(s.ctx))
	}
}

// TestMigrate_OnCleanDatabase - основной сценарий: библиотека накатывает
// схему сама.
func (s *MigrationsSuite) TestMigrate_OnCleanDatabase() {
	t := s.T()
	dsn := s.freshDatabase()

	require.NoError(t, mediaservice.Migrate(dsn))

	// Проверяем не факт возврата nil, а появившиеся таблицы: Migrate мог бы
	// вернуть nil и не сделав ничего.
	tables := s.tablesOf(dsn)
	require.Contains(t, tables, "media")
	require.Contains(t, tables, "media_derivative")
	require.Contains(t, tables, "schema_migrations",
		"golang-migrate не завёл служебную таблицу - миграции не применялись")
}

// TestMigrate_Idempotent - повторный вызов на уже мигрированной базе
// не ошибка. Внутри это ErrNoChange, который библиотека обязана проглотить.
func (s *MigrationsSuite) TestMigrate_Idempotent() {
	t := s.T()
	dsn := s.freshDatabase()

	require.NoError(t, mediaservice.Migrate(dsn))
	require.NoError(t, mediaservice.Migrate(dsn),
		"повторное применение миграций должно быть безвредным")
}

// TestMigrations_ExternalMigrator - второй способ из критериев приёмки:
// встраивающий проект берёт файлы через Migrations() и применяет их своим
// мигратором. Схема обязана получиться той же.
func (s *MigrationsSuite) TestMigrations_ExternalMigrator() {
	t := s.T()

	byLibrary := s.freshDatabase()
	require.NoError(t, mediaservice.Migrate(byLibrary))

	external := s.freshDatabase()
	s.applyExternally(external)

	// Сравниваем не «есть ли таблицы», а полный список: так тест поймает
	// и лишнюю таблицу, и недостающую.
	require.Equal(t, s.tablesOf(byLibrary), s.tablesOf(external),
		"внешний мигратор дал другую схему, чем библиотека")
}

// TestMigrateContext_Canceled - отменённый контекст останавливает работу
// до подключения к базе.
func (s *MigrationsSuite) TestMigrateContext_Canceled() {
	t := s.T()
	dsn := s.freshDatabase()

	ctx, cancel := context.WithCancel(s.ctx)
	cancel() // отменяем до вызова: проверяем самую раннюю ветку

	err := mediaservice.MigrateContext(ctx, dsn)
	require.ErrorIs(t, err, context.Canceled)

	// Схема не должна была появиться.
	require.NotContains(t, s.tablesOf(dsn), "media",
		"миграции применились, несмотря на отменённый контекст")
}

// --- вспомогательное ---

func (s *MigrationsSuite) dsnFor(db string) string {
	return fmt.Sprintf("postgres://media:media@%s:%s/%s?sslmode=disable",
		s.host, s.port, db)
}

// freshDatabase создаёт пустую базу со случайным именем и возвращает DSN.
// Каждый тест работает в своей базе, поэтому порядок выполнения не важен
// и тесты не мешают друг другу.
func (s *MigrationsSuite) freshDatabase() string {
	s.T().Helper()

	// Дефис в имени базы потребовал бы кавычек в CREATE DATABASE,
	// поэтому убираем его из UUID.
	name := "mig_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	_, err := s.admin.Exec(s.ctx, "CREATE DATABASE "+name)
	require.NoError(s.T(), err)

	return s.dsnFor(name)
}

// tablesOf возвращает отсортированный список таблиц публичной схемы.
func (s *MigrationsSuite) tablesOf(dsn string) []string {
	s.T().Helper()

	pool, err := pgxpool.New(s.ctx, dsn)
	require.NoError(s.T(), err)
	defer pool.Close()

	rows, err := pool.Query(s.ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public'
		ORDER BY table_name`)
	require.NoError(s.T(), err)
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		require.NoError(s.T(), rows.Scan(&name))
		out = append(out, name)
	}
	require.NoError(s.T(), rows.Err())
	return out
}

// applyExternally применяет миграции так, как это сделал бы встраивающий
// проект: берёт файлы через публичный Migrations() и запускает свой мигратор.
func (s *MigrationsSuite) applyExternally(dsn string) {
	s.T().Helper()
	t := s.T()

	src, err := iofs.New(mediaservice.Migrations(), ".")
	require.NoError(t, err)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	require.NoError(t, err)

	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	require.NoError(t, err)
	defer func() { _, _ = m.Close() }()

	require.NoError(t, m.Up())
}
