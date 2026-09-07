package mediaservice_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"mediaservice/pkg/mediaservice"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
)

// Значения ниже подразумеваются уже подготовленными вызывающим кодом.
// В примерах они нужны, чтобы файл компилировался, и читаются как
// "ваш клиент", "ваш пул соединений", "идентификатор владельца".
var (
	client  *mediaservice.Client
	pool    *pgxpool.Pool
	mc      *minio.Client
	ownerID uuid.UUID
	mediaID uuid.UUID
)

// Библиотека сама создаёт пул Postgres и клиент MinIO и сама закрывает их
// в Close(). Опция WithAutoMigrate применит схему при старте.
func ExampleNew() {
	ctx := context.Background()

	client, err := mediaservice.New(ctx, mediaservice.Config{
		PostgresDSN: os.Getenv("POSTGRES_DSN"),
		MinIO: mediaservice.MinIOConfig{
			Endpoint:  "localhost:9000",
			AccessKey: os.Getenv("MINIO_ACCESS_KEY"),
			SecretKey: os.Getenv("MINIO_SECRET_KEY"),
			Bucket:    "media",
		},
	}, mediaservice.WithAutoMigrate(), mediaservice.WithPresignTTL(15*time.Minute))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	m, err := client.GetMedia(ctx, ownerID, mediaID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(m.MIMEType, m.SizeBytes, m.Status)
}

// Если у встраивающего проекта уже есть свои соединения, передайте их через
// NewWithDeps: Close() их не тронет.
func ExampleNewWithDeps() {
	ctx := context.Background()

	// pool и mc созданы приложением и живут дольше клиента.
	client, err := mediaservice.NewWithDeps(ctx, mediaservice.Deps{
		Pool:   pool,
		MinIO:  mc,
		Bucket: "media",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
}

// Временная ссылка на скачивание. Срок жизни задаётся при создании клиента
// опцией WithPresignTTL, а не аргументом метода.
func ExampleClient_GetDownloadURL() {
	ctx := context.Background()

	url, err := client.GetDownloadURL(ctx, ownerID, mediaID, mediaservice.VariantThumb)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(url.URL, "истекает", url.ExpiresAt)
}

// Содержимое отдаётся потоком: файл не поднимается в память целиком.
// Закрыть поток обязан вызывающий.
func ExampleClient_DownloadStream() {
	ctx := context.Background()

	rc, err := client.DownloadStream(ctx, ownerID, mediaID, mediaservice.VariantOriginal)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = rc.Close() }()

	if _, err := io.Copy(os.Stdout, rc); err != nil {
		log.Fatal(err)
	}
}

// Если схемой управляет сам встраивающий проект, возьмите файлы миграций
// через Migrations() и примените своим мигратором.
func ExampleMigrations() {
	src, err := iofs.New(mediaservice.Migrations(), ".")
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("pgx", os.Getenv("POSTGRES_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		log.Fatal(err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatal(err)
	}
}
