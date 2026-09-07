package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"

	"mediaservice/migrations"
)

// RunMigrationsContext применяет встроенные миграции схемы к базе по dsn.
//
// Контекст соблюдается частично: golang-migrate не принимает его в Up(),
// поэтому отмена возможна только на границе версий. Уже начавшийся SQL
// одной миграции досчитается до конца - прервать длинный ALTER TABLE
// нельзя. Это ограничение библиотеки миграций, а не недосмотр.
func RunMigrationsContext(ctx context.Context, dsn string) error {
	if dsn == "" {
		return fmt.Errorf("repo: DSN is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("repo: load embedded migrations: %w", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("repo: open database: %w", err)
	}

	// sql.Open соединение не устанавливает. Пингуем явно, чтобы недоступная
	// база выяснилась здесь, а не внутри мигратора, и чтобы на этом шаге
	// работала отмена по контексту.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("repo: connect database: %w", err)
	}

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("repo: init migrate driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("repo: init migrator: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	// Up() контекста не принимает, поэтому запускаем его отдельно и ждём
	// либо завершения, либо отмены. GracefulStop просит мигратор
	// остановиться после текущей версии.
	done := make(chan error, 1)
	go func() { done <- m.Up() }()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("repo: apply migrations: %w", err)
		}
		return nil
	case <-ctx.Done():
		m.GracefulStop <- true
		// Дожидаемся выхода из Up(), иначе m.Close() в defer гонится
		// с работающей горутиной.
		<-done
		return ctx.Err()
	}
}

// RunMigrations применяет встроенные миграции схемы к базе по dsn.
//
// Эквивалент RunMigrationsContext с context.Background().
func RunMigrations(dsn string) error {
	return RunMigrationsContext(context.Background(), dsn)
}
