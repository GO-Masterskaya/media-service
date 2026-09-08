# Media Service

Коробочный сервис хранения медиа: приём картинок, видео и аудио, обработка
через ffmpeg, бинари в MinIO, метаданные в Postgres. Медиа привязывается к
внешнему `owner_id`, который передаёт вызывающий проект.

Внешний контракт — только gRPC. Kafka-слушатель опционален, включается тоглом.

## Документация

- [ТЗ](docs/TZ.md) — требования и критерии приёмки
- [SPEC](docs/SPEC.md) — стек, структура, gRPC-контракт, схема БД, конфиг

## Стек

Go · gRPC · PostgreSQL · MinIO · ffmpeg · Kafka (опционально) · Docker Compose

## Структура

```
cmd/mediaservice   точка входа
internal/api       gRPC-хендлеры
internal/media     доменная логика
internal/storage   адаптер MinIO
internal/repo      репозитории Postgres
internal/processing воркер-пул и ffmpeg
internal/events    Kafka consumer
internal/config    конфиг из env
proto/media/v1     gRPC-контракт
migrations         миграции БД
```

## Быстрый старт

```bash
cp .env.example .env
make up
make test
```

## Команды

```bash
make build   # сборка
make lint    # линтер
make test    # тесты
make help    # все команды
```

## Миграции

Схема БД меняется **только новыми миграциями** — уже применённые файлы в
`migrations/` править нельзя. Это гарантирует воспроизводимость и безопасный
повторный накат на любой БД.

- Формат имён: `NNNNNN_описание.up.sql` и парный `NNNNNN_описание.down.sql`
  (например `000002_add_tags.up.sql`). Номер — следующий по порядку.
- Каждая `up`-миграция должна иметь обратную `down`, полностью её откатывающую.
- Файлы встраиваются в бинарь через `//go:embed` (см. `migrations/embed.go`),
  поэтому новые `*.sql` подхватываются автоматически — код менять не нужно.
- В standalone-режиме миграции применяются автоматически при старте сервиса
  (`repo.RunMigrations`). При встраивании как библиотеки схемой управляет
  вызывающее приложение.

## Proto toolchain

Контракт описан в `proto/media/v1/media.proto`. Генерация Go stubs воспроизводится
через `buf` + локальные плагины `protoc-gen-go` / `protoc-gen-go-grpc`.

### Установка

```bash
# 1. buf — управляет зависимостями proto и вызывает плагины
go install github.com/bufbuild/buf/cmd/buf@latest

# 2. Go плагины для protoc
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 3. Рантайм-валидатор
go get buf.build/go/protovalidate
```

Валидация buf.validate выполняется автоматически в unary/streaming interceptor до попадания в handler

В корне репозитория уже есть `buf.yaml` и `buf.gen.yaml` — они фиксируют
версии плагинов и зависимостей proto. Менять их не нужно для повторной генерации.

### Использование сгенерированных stubs

Сгенерированные типы лежат в `proto/media/v1` (пакет `mediav1`). Пример клиента:

```go
import (
    "google.golang.org/grpc"
    mediav1 "mediaservice/proto/media/v1"
)

conn, _ := grpc.Dial("localhost:9090", grpc.WithTransportCredentials(...))
client := mediav1.NewMediaServiceClient(conn)
resp, _ := client.GetMedia(ctx, &mediav1.GetMediaRequest{MediaId: "..."})
```
