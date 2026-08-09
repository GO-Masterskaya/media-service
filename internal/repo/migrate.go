package repo

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"

	"mediaservice/migrations"
)

func RunMigrations(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("repo: DSN is required")
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("repo: load embedded migrations: %w", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("repo: open database: %w", err)
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

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("repo: apply migrations: %w", err)
	}

	return nil
}
