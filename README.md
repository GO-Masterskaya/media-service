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

Для локальной проверки Kafka запустите профиль `kafka`: `docker compose --profile kafka up -d`.
Контейнер `createkafkatopics` создаёт `media.events` и `media.events.dlq`; для
потребления задайте `KAFKA_ENABLED=true`.

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
