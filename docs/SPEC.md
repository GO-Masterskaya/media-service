# SPEC — Media Service (Go)

Техническая спека к [TZ.md](TZ.md). Публичный контракт — gRPC proto. Стек,
структура, схема БД, конфиг, пайплайн.

---

## 1. Стек
- Go 1.22+, `module mediaservice`.
- gRPC: `google.golang.org/grpc`, `protoc-gen-go`, `protoc-gen-go-grpc`. Валидация — `protovalidate` (или buf validate).
- MinIO: `github.com/minio/minio-go/v7`.
- Postgres: `pgx/v5` + `pgxpool`; миграции — `goose` или `golang-migrate`.
- Kafka: `github.com/twmb/franz-go` (или `segmentio/kafka-go`).
- ffmpeg/ffprobe: вызов через `os/exec` (аргументы массивом, без shell). Бинарь ffmpeg — в образе.
- Метрики: `prometheus/client_golang`. Логи: `log/slog`.
- Тесты: `testcontainers-go` (Postgres, MinIO), `stretchr/testify`.

## 2. Структура проекта
```
cmd/mediaservice/main.go       — сборка зависимостей, запуск gRPC + воркеров
internal/
  api/            — gRPC handlers (тонкий слой, маппинг proto<->domain)
  media/          — доменная логика (upload, delete, статусы)
  storage/        — MinIO адаптер (put, presign, delete)
  repo/           — Postgres репозитории (media, derivative, jobs)
  processing/     — воркер-пул, ffmpeg/ffprobe обёртки, job runner
  events/         — Kafka consumer (за тоглом)
  config/         — env-конфиг
pkg/              — то, что реально переиспользуемо наружу (клиент, ошибки)
proto/media/v1/media.proto
migrations/
docker-compose.yml
.env.example
```

## 3. gRPC контракт (эскиз)

```proto
syntax = "proto3";
package media.v1;
option go_package = "mediaservice/proto/media/v1;mediav1";

service MediaService {
  rpc Upload(stream UploadRequest) returns (UploadResponse);          // client-streaming
  rpc GetMedia(GetMediaRequest) returns (Media);
  rpc ListMediaByOwner(ListMediaByOwnerRequest) returns (ListMediaByOwnerResponse);
  rpc GetDownloadURL(GetDownloadURLRequest) returns (GetDownloadURLResponse);
  rpc DownloadStream(DownloadStreamRequest) returns (stream DownloadChunk); // server-streaming, отдаём файл
  rpc DeleteMedia(DeleteMediaRequest) returns (DeleteMediaResponse);
  rpc DeleteByOwner(DeleteByOwnerRequest) returns (DeleteByOwnerResponse);
}

message UploadRequest {
  oneof payload {
    UploadInit init = 1;   // ровно первым сообщением
    bytes chunk = 2;       // далее — байты
  }
}

message UploadInit {
  string owner_id = 1;              // UUID, required
  string filename = 2;
  string mime = 3;                  // заявленный
  uint64 expected_size = 4;
  string idempotency_key = 5;       // required
  ProcessingOptions processing = 6;
  google.protobuf.Duration ttl = 7; // опц. TTL; не задан = хранить вечно
}

message ProcessingOptions {
  bool make_thumbnail = 1;
  bool transcode = 2;               // v1: единственная рендиция 720p
}

message UploadResponse { string media_id = 1; MediaStatus status = 2; }

enum MediaKind   { KIND_UNSPECIFIED=0; IMAGE=1; VIDEO=2; AUDIO=3; }
enum MediaStatus { STATUS_UNSPECIFIED=0; STORED=1; PROCESSING=2; READY=3; FAILED=4; DELETING=5; }

message Media {
  string id = 1; string owner_id = 2; MediaKind kind = 3;
  string mime = 4; uint64 size_bytes = 5; MediaStatus status = 6;
  google.protobuf.Struct metadata = 7;         // ffprobe-мета
  repeated Derivative derivatives = 8;
  string error = 9;
  google.protobuf.Timestamp created_at = 10;
}

message Derivative { string variant=1; string mime=2; uint64 size_bytes=3; }

message GetMediaRequest { string media_id = 1; }

message ListMediaByOwnerRequest { string owner_id=1; uint32 page_size=2; string page_token=3; }
message ListMediaByOwnerResponse { repeated Media items=1; string next_page_token=2; }

message GetDownloadURLRequest { string media_id=1; string variant=2; } // original|thumbnail|r_720
message GetDownloadURLResponse { string url=1; google.protobuf.Timestamp expires_at=2; }

message DownloadStreamRequest { string media_id=1; string variant=2; }
message DownloadChunk { bytes data=1; }

message DeleteMediaRequest { string media_id=1; }
message DeleteMediaResponse { bool deleted=1; }
message DeleteByOwnerRequest { string owner_id=1; }
message DeleteByOwnerResponse { uint32 deleted_count=1; }
```
Ошибки — стандартные gRPC codes: `InvalidArgument` (валидация), `NotFound`, `AlreadyExists` (idempotency конфликт с другим телом), `FailedPrecondition`, `Internal`.

## 4. Пайплайн upload (последовательность)
```
client stream ──> handler
  1. recv UploadInit           validate(owner uuid, mime allowlist, size<=limit)
  2. check idempotency (owner, key) -> если есть: вернуть media_id, drain stream
  3. stream chunks -> temp file (io.Copy с лимитом)
  4. ffprobe(temp)             -> kind, duration, w/h, codec, bitrate
     mismatch(kind vs mime class) -> InvalidArgument, cleanup
  5. minio.Put(original)       key = {owner}/{media_id}/original.{ext}
  6. repo.InsertMedia(status=STORED, metadata)
  7. if processing requested:
        repo.EnqueueJob(thumbnail|transcode); status=PROCESSING
  8. return {media_id, status}
```

## 5. Горутин-движок обработки

Не polling-луп, а движок на каналах + пул воркеров. Компоненты:

```
Engine
├─ jobCh    chan Job              // буферизованный, размер = QUEUE_BUFFER
├─ feeder   goroutine             // доливает из БД в jobCh при свободных слотах
├─ workers  [WORKER_CONCURRENCY]  // читают jobCh, гоняют ffmpeg
├─ reaper   goroutine             // TTL-чистка (expires_at<=now)
└─ ctx, errgroup                  // единый lifecycle, graceful drain
```

Логика воркера:
```
for job := range jobCh {
  ctx := context.WithTimeout(engineCtx, FFMPEG_TIMEOUT)
  switch job.type {
    thumbnail: ffmpeg -> temp -> minio.Put(thumb) -> repo.InsertDerivative
    transcode: ffmpeg 720p -> minio.Put(r_720) -> repo.InsertDerivative
  }
  on success: repo.MarkJobDone; if last job -> media.status=READY
  on error:   attempts++; if attempts<max: repo.Reschedule(backoff, status=queued)
              else repo.MarkFailed + media.status=FAILED(error)
}
```

Feeder:
```
for {
  free := cap(jobCh) - len(jobCh)
  if free > 0 {
    jobs := repo.ClaimQueued(free)   // UPDATE ... SET status=running WHERE status=queued ... RETURNING (batch)
    for j := range jobs { jobCh <- j }
  }
  select { case <-ctx.Done(): return; case <-tick: }   // короткий интервал/сигнал
}
```

- **Startup recovery**: `UPDATE processing_jobs SET status=queued WHERE status=running` (сервис упал — задачи вернуть).
- **Graceful shutdown**: `cancel(ctx)` → feeder стоп, `close(jobCh)`, воркеры доделывают текущее в пределах `SHUTDOWN_TIMEOUT`, недоделанное остаётся `running` → при следующем старте recovery вернёт в `queued`. `errgroup.Wait()`.
- **Метрики**: `len(jobCh)`, in-flight воркеров, длительность ffmpeg, ретраи, размер БД-очереди.
- **Без утечек**: все горутины под `errgroup`/`ctx`; временные файлы `defer os.Remove` + чистка на панике.
- **Reaper**: тик по `TTL_REAP_INTERVAL`, `SELECT ... WHERE expires_at<=now` → `DeleteMedia` каждому.

ffmpeg — аргументы массивом, без `sh -c`, `-nostdin -y`, таймаут через `context`:
- probe: `ffprobe -v quiet -print_format json -show_format -show_streams <in>`
- thumb video: `ffmpeg -nostdin -y -ss 1 -i <in> -frames:v 1 -vf scale='min(320,iw)':-2 <out.jpg>`
- transcode 720: `ffmpeg -nostdin -y -i <in> -vf scale=-2:720 -c:v libx264 -preset veryfast -c:a aac <out.mp4>`
- waveform: `ffmpeg -nostdin -y -i <in> -filter_complex showwavespic=s=640x120 -frames:v 1 <out.png>`

## 6. Схема БД
См. [TZ.md §6](TZ.md). Таблицы: `media`, `media_derivative`, `processing_jobs`.
Индексы: `media(owner_id)`, `unique media(owner_id, idempotency_key)`,
`processing_jobs(status, run_after)`. Пагинация ListByOwner — keyset по `(created_at, id)`.

## 7. Конфиг (env, `.env.example`)
```
GRPC_ADDR=:9090
GRPC_AUTH_TOKEN=change-me            # token обязателен в контракте; v1 НЕ валидирует (пробрасывается)
MAX_UPLOAD_BYTES=524288000           # 500MB
MIME_ALLOWLIST=image/*,video/*,audio/*
WORKER_CONCURRENCY=2                  # горутин-воркеров обработки
QUEUE_BUFFER=64                       # размер jobCh
FFMPEG_TIMEOUT=10m
SHUTDOWN_TIMEOUT=30s
RENDITION=720                         # v1: единственная рендиция
THUMB_SECOND=1
PRESIGN_TTL=15m
TTL_REAP_INTERVAL=1m                  # период reaper'а авто-удаления
RATE_LIMIT_RPS=50                     # per-caller
MAX_CONCURRENT_STREAMS=8              # per-caller upload/download стримов

POSTGRES_DSN=postgres://media:media@postgres:5432/media?sslmode=disable

MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=media
MINIO_USE_SSL=false

KAFKA_ENABLED=false
KAFKA_BROKERS=kafka:9092
KAFKA_TOPIC=media.events
KAFKA_DLQ_TOPIC=media.events.dlq
KAFKA_GROUP=media-service
```

## 8. Kafka consumer (за тоглом)
- Стартует только при `KAFKA_ENABLED=true`.
- Событие — **JSON**:
```json
{ "type": "detach", "owner_id": "uuid", "media_id": "uuid", "event_id": "uuid" }
```
- `detach` → `media.DeleteMedia(media_id)` (та же доменная функция).
- `attach` → проставить/подтвердить `owner_id`.
- Идемпотентность по `event_id` (таблица обработанных или natural idempotency delete).
- Commit offset только после успеха.
- **DLQ**: после max ретраев событие публикуется в `KAFKA_DLQ_TOPIC` (с причиной в header), offset основного топика коммитится — поток не встаёт.

## 9. docker-compose (состав)
`mediaservice` (build ., ffmpeg в образе) + `postgres` + `minio` (+ createbucket init) + опц. `kafka`+`zookeeper`/`redpanda` профилем `kafka`. Kafka — под `profiles: [kafka]`, чтобы дефолтный up её не поднимал.

## 10. Что проверять на ревью (спец. для этого сервиса)
- Стрим (upload и DownloadStream) не буферизуется целиком в RAM (io.Copy в temp/minio, лимит).
- ffmpeg-аргументы не из пользовательских строк; нет `sh -c`; есть таймаут/контекст; temp-файлы чистятся (defer + на паниках).
- Presigned bucket реально private; TTL короткий; filename не утекает в ключ.
- Idempotency работает под гонкой (unique constraint, не read-then-write).
- Удаление (жёсткое) согласовано БД↔MinIO; нет «сирот» при падении между шагами (uborka).
- **Горутин-движок**: нет утечек горутин (всё под ctx/errgroup); канал не переполняется без backpressure; recovery на старте реально возвращает `running`→`queued`; конкуррентность не превышает `WORKER_CONCURRENCY`; graceful drain корректен.
- **TTL reaper**: истёкшее удаляется; не задан ttl → не трогается.
- Kafka: commit после обработки, не до; DLQ не глотает событие молча (причина в header/логе); тогл реально отключает коннект.
- Rate limit / max streams enforced per caller, не глобально мимо.
- Token: interceptor читает, но v1 намеренно не реджектит — не перепутать с «забыли проверить».
- Graceful shutdown дренит воркеры и стримы.
- Ошибки ffmpeg не роняют сервис, ведут в `failed` с причиной.
