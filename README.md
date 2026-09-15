# Media Service

Коробочный сервис хранения медиа: приём картинок, видео и аудио, обработка
через ffmpeg, бинари в MinIO, метаданные в Postgres. Медиа привязывается к
внешнему `owner_id`, который передаёт вызывающий проект.

Подключается двумя способами: вызовом по gRPC и встраиванием Go-библиотекой
`pkg/mediaservice`. Kafka-слушатель опционален, включается тоглом.

## Документация

Интегратору:

- [Подключение](docs/INTEGRATION.md) - с чего начать, оба способа, сквозной сценарий
- [gRPC API](docs/API.md) - контракт, коды ошибок, идемпотентность, повторные попытки
- [Конфигурация](docs/CONFIG.md) - все env: назначение, default, security note
- [Runbooks](docs/RUNBOOKS.md) - ENOSPC, зависшая обработка, сверка удалений, Kafka DLQ

Разработчику:

- [ТЗ](docs/TZ.md) - требования и критерии приёмки
- [SPEC](docs/SPEC.md) - стек, структура, gRPC-контракт, схема БД, конфиг

Где SPEC расходится с поведением кода, права реализация: известные расхождения
перечислены в конце [docs/API.md](docs/API.md).

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
internal/upload    приём во временное хранилище
internal/config    конфиг из env
pkg/mediaservice   встраиваемая библиотека
proto/media/v1     gRPC-контракт
migrations         миграции БД
```

## Быстрый старт

```bash
cp .env.example .env
make up
curl -fsS localhost:8080/readyz
```

Порты: `9090` - gRPC, `8080` - health-пробы (`/livez`, `/readyz`) и `/metrics`.

Перед использованием вне своей машины поменять `GRPC_AUTH_TOKEN`, ключи MinIO
и пароль в `POSTGRES_DSN`: дефолты общеизвестные. См.
[docs/CONFIG.md](docs/CONFIG.md).

## Команды

```bash
make build   # сборка
make lint    # линтер
make test    # тесты
make help    # все команды
```

## Встраивание библиотекой

```bash
go get github.com/GO-Masterskaya/media-service
```

```go
import "github.com/GO-Masterskaya/media-service/pkg/mediaservice"

client, err := mediaservice.New(ctx, mediaservice.Config{
    PostgresDSN: os.Getenv("POSTGRES_DSN"),
    MinIO: mediaservice.MinIOConfig{
        Endpoint:  "localhost:9000",
        AccessKey: os.Getenv("MINIO_ACCESS_KEY"),
        SecretKey: os.Getenv("MINIO_SECRET_KEY"),
        Bucket:    "media",
    },
}, mediaservice.WithAutoMigrate())
if err != nil {
    return err
}
defer func() { _ = client.Close() }()

res, err := client.Upload(ctx, mediaservice.UploadParams{
    OwnerID:        ownerID,
    Filename:       "photo.jpg",
    MIMEType:       "image/jpeg",
    IdempotencyKey: key,
}, file)
```

Библиотека сама создаёт пул Postgres и клиент MinIO и сама закрывает их в
`Close()`. Если соединения у приложения уже есть, передайте их через
`NewWithDeps` - тогда `Close()` их не тронет.

Ограничения встроенного режима (ffmpeg в вашем процессе, reaper и reconciler не
запускаются) описаны в [docs/INTEGRATION.md](docs/INTEGRATION.md). Компилируемые
примеры на все основные методы - в `pkg/mediaservice/example_test.go`.

## Вызов по gRPC

Сервер не регистрирует gRPC reflection, поэтому схему для `grpcurl` нужно
собрать заранее:

```bash
buf build -o media.protoset.binpb
grpcurl -protoset media.protoset.binpb -plaintext \
  -H "authorization: Bearer $GRPC_AUTH_TOKEN" \
  localhost:9090 list media.v1.MediaService
```

Go-клиентам генерировать ничего не нужно: stubs лежат в `proto/media/v1`
(пакет `mediav1`).

```go
import mediav1 "github.com/GO-Masterskaya/media-service/proto/media/v1"

conn, err := grpc.NewClient("localhost:9090",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
    return err
}
defer func() { _ = conn.Close() }()

client := mediav1.NewMediaServiceClient(conn)
resp, err := client.GetMedia(ctx, &mediav1.GetMediaRequest{MediaId: mediaID})
```

Примеры на каждый RPC - в [docs/API.md](docs/API.md).

## Миграции

Схема БД меняется **только новыми миграциями** - уже применённые файлы в
`migrations/` править нельзя. Это гарантирует воспроизводимость и безопасный
повторный накат на любой БД.

- Формат имён: `NNNNNN_описание.up.sql` и парный `NNNNNN_описание.down.sql`
  (например `000002_add_tags.up.sql`). Номер - следующий по порядку.
- Каждая `up`-миграция должна иметь обратную `down`, полностью её откатывающую.
- Файлы встраиваются в бинарь через `//go:embed` (см. `migrations/embed.go`),
  поэтому новые `*.sql` подхватываются автоматически - код менять не нужно.
- В standalone-режиме миграции применяются автоматически при старте сервиса
  (`repo.RunMigrations`). При встраивании как библиотеки схемой управляет
  вызывающее приложение: `WithAutoMigrate()`, `Migrate(dsn)` или `Migrations()`
  для своего мигратора.

## Proto toolchain

Контракт описан в `proto/media/v1/media.proto`. Генерация Go stubs воспроизводится
через `buf` + локальные плагины `protoc-gen-go` / `protoc-gen-go-grpc`.

### Установка

```bash
# 1. buf — управляет зависимостями proto и вызывает плагины
#    (в CI: bufbuild/buf-action version 1.72.0)
go install github.com/bufbuild/buf/cmd/buf@v1.72.0

# 2. Go плагины для protoc — те же пины, что в CI / Makefile
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

# 3. Рантайм-валидатор (версия из go.mod / buf.lock, не buf dep update в CI)
go get buf.build/go/protovalidate
```

Валидация buf.validate выполняется автоматически в unary/streaming interceptor до попадания в handler

В корне репозитория уже есть `buf.yaml` и `buf.gen.yaml` — они фиксируют
версии плагинов и зависимостей proto. Менять их не нужно для повторной генерации.

`make proto` и `buf build` ходят в Buf Schema Registry за
`buf.build/bufbuild/protovalidate`: сам `.proto` этой зависимости в репозитории
нет, в отличие от её Go-рантайма, который приезжает модулем. Поэтому `go build`
работает офлайн, а генерация - нет. Под VPN BSR может отвечать 403 (Cloudflare
режет выходные узлы); признак - `buf registry whoami` тоже отдаёт 403.