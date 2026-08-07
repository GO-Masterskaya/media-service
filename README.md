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
