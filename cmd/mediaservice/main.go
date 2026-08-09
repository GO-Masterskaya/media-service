package main

import (
	"context"
	"log"
	"os"
	"time"

	"mediaservice/internal/repo"
)

func main() {

	// TODO(#3): заменить чтение из переменной окружения на пакет конфигурации,
	// который также будет источником таймаутов ниже.
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("mediaservice: DATABASE_URL is required")
	}

	if err := repo.RunMigrations(dsn); err != nil {
		log.Fatalf("mediaservice: run migrations: %v", err)
	}

	ctx := context.Background()
	pool, err := repo.NewPool(ctx, repo.PoolConfig{
		DSN:            dsn,
		ConnectTimeout: 5 * time.Second,
		QueryTimeout:   30 * time.Second,
	})
	if err != nil {
		log.Fatalf("mediaservice: create pool: %v", err)
	}
	defer pool.Close()

	// TODO: запустить gRPC-сервис.
	log.Println("mediaservice: migrations applied, connection pool ready")
}
