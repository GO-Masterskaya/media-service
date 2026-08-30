package mediaservice

import (
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

// Migrate применяет миграции схемы к указанной базе.
//
// Альтернатива опции WithAutoMigrate: та применяет миграции при создании
// клиента, а Migrate можно вызвать отдельно, например из скрипта
// развёртывания, не создавая клиент.
func Migrate(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("dsn is required: %w", ErrInvalidArgument)
	}
	return repo.RunMigrations(dsn)
}
