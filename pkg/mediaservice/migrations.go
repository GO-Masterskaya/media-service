package mediaservice

import (
	"context"
	"fmt"
	"io/fs"

	"mediaservice/internal/repo"
	"mediaservice/migrations"
)

// Migrations возвращает файлы миграций схемы для применения внешним
// мигратором встраивающего проекта.
//
// Имена файлов в формате golang-migrate: {версия}_{имя}.{up|down}.sql.
// Пример применения через golang-migrate:
//
//	src, _ := iofs.New(mediaservice.Migrations(), ".")
//	m, _ := migrate.NewWithSourceInstance("iofs", src, dsn)
//	err := m.Up()
func Migrations() fs.FS {
	return migrations.FS
}

// MigrateContext применяет миграции схемы к указанной базе.
//
// Контекст соблюдается частично: используемая библиотека миграций не
// принимает его напрямую, поэтому отмена срабатывает на границе версий.
// Уже начавшаяся миграция досчитается до конца.
func MigrateContext(ctx context.Context, dsn string) error {
	if dsn == "" {
		return fmt.Errorf("dsn is required: %w", ErrInvalidArgument)
	}
	if err := repo.RunMigrationsContext(ctx, dsn); err != nil {
		return mapCoreError(err)
	}
	return nil
}

// Migrate применяет миграции схемы к указанной базе.
//
// Эквивалент MigrateContext с context.Background(): удобно вызывать
// из скриптов развёртывания, где отмена не нужна.
//
// Альтернатива опции WithAutoMigrate: та применяет миграции при создании
// клиента, а Migrate можно вызвать отдельно, не создавая клиент.
func Migrate(dsn string) error {
	return MigrateContext(context.Background(), dsn)
}
