# Подключение сервиса

Документ для того, кто впервые подключает media-service к своему проекту.
Задача - за один проход поднять сервис и провести файл через полный цикл:
загрузка, ожидание обработки, скачивание.

Дальше по темам:

- [`CONFIG.md`](CONFIG.md) - все переменные окружения
- [`API.md`](API.md) - gRPC-контракт, коды ошибок, повторные попытки
- [`RUNBOOKS.md`](RUNBOOKS.md) - что делать, когда что-то сломалось
- [`SPEC.md`](SPEC.md), [`TZ.md`](TZ.md) - замысел и требования

Где `SPEC.md` расходится с поведением кода, права реализация. Известные
расхождения перечислены в конце `API.md`.

---

## Два способа подключения

Сервис можно вызывать по сети или встроить в свой бинарь как библиотеку. Это
не два уровня зрелости, а два разных решения с разными последствиями.

| | gRPC-сервис | Библиотека `pkg/mediaservice` |
|---|---|---|
| Что разворачиваете | Процесс плюс Postgres и MinIO | Только Postgres и MinIO |
| Язык вызывающего | Любой, у кого есть gRPC | Только Go |
| Границы отказа | Сервис падает отдельно от вас | Падает вместе с вашим процессом |
| Масштабирование | Независимое | Вместе с вашим приложением |
| Обработка ffmpeg | В сервисе, ffmpeg в его образе | **В вашем процессе** |
| Проверка владельца | Через `x-owner-id`, сервис его не верифицирует | Всегда строгая: `CallerID = OwnerID` |
| TTL и уборка | Reaper и reconciler работают | **Не работают**: их поднимает только запущенный сервис |
| `DeleteByOwner` | `UNIMPLEMENTED` | Работает |

**Выбирайте gRPC**, если вызывающих несколько или они не на Go, если медиа -
самостоятельная часть системы, если нагрузка от обработки должна масштабироваться
отдельно, или если нужны TTL и фоновая уборка из коробки.

**Выбирайте библиотеку**, если у вас один Go-монолит, сетевой hop к медиа
выглядит лишним, и вы готовы, чтобы ffmpeg работал внутри вашего процесса и
конкурировал с ним за CPU.

Ниже разобраны оба пути. Схема БД и хранилища одна и та же, так что переход
между ними не требует миграции данных.

---

## Путь 1: gRPC-сервис

### Быстрый старт

```bash
git clone https://github.com/GO-Masterskaya/media-service.git
cd media-service
cp .env.example .env
make up
```

`make up` поднимает Postgres, MinIO, создаёт приватный бакет и запускает сервис.
Миграции накатываются автоматически на старте: они встроены в бинарь через
`//go:embed`, каталог `migrations/` в рантайме не нужен.

Проверка, что всё поднялось:

```bash
curl -fsS localhost:8080/readyz     # {"status":"ready"}
docker compose logs mediaservice | grep "all components started successfully"
```

Что править в `.env` перед любым использованием вне своей машины: `GRPC_AUTH_TOKEN`,
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, пароль в `POSTGRES_DSN`. Дефолты там
общеизвестные - подробности в [`CONFIG.md`](CONFIG.md).

Порты: `9090` - gRPC, `8080` - health-пробы `/livez`, `/readyz` и `/metrics`.

### Подготовка grpcurl

Сервер не регистрирует gRPC reflection, а контракт импортирует
`buf/validate/validate.proto`, которого нет в репозитории. Поэтому схему надо
принести с собой:

```bash
buf build -o media.protoset.binpb
export GRPCURL="grpcurl -protoset media.protoset.binpb -plaintext"
```

Первый запуск `buf build` требует сети до Buf Schema Registry, дальше работает
из кеша. Проверка:

```bash
$GRPCURL localhost:9090 list media.v1.MediaService
```

### Сквозной сценарий

```bash
TOKEN=change-me
OWNER=$(uuidgen)
AUTH=(-H "authorization: Bearer $TOKEN" -H "x-owner-id: $OWNER")
FILE=photo.jpg
SIZE=$(stat -c%s "$FILE")
KEY=$(uuidgen)
```

**1. Загрузить.** Первое сообщение потока - `init`, дальше чанки. В примере
файл уходит одним чанком; настоящий клиент режет поток сам, потому что потолок
одного protobuf-сообщения - 16 МиБ.

```bash
{
  jq -nc --arg o "$OWNER" --arg f "$FILE" --argjson s "$SIZE" --arg k "$KEY" \
    '{init: {owner_id: $o, filename: $f, mime: "image/jpeg",
             expected_size: $s, idempotency_key: $k,
             processing: {make_thumbnail: true}}}'
  jq -nc --arg c "$(base64 -w0 "$FILE")" '{chunk: $c}'
} | $GRPCURL "${AUTH[@]}" -d @ localhost:9090 media.v1.MediaService/Upload
```

```json
{ "mediaId": "6a2b...", "status": "PROCESSING" }
```

**2. Дождаться обработки.** Push-уведомлений нет, статус узнаётся опросом.

```bash
MEDIA=6a2b...
$GRPCURL "${AUTH[@]}" -d "{\"media_id\":\"$MEDIA\"}" \
  localhost:9090 media.v1.MediaService/GetMedia
```

`status` пройдёт `PROCESSING` -> `READY` либо `FAILED` (причина - в поле
`error`). Начинать опрос с секунды и растить интервал до десяти.

Если обработку **не** просили, объект останется в статусе `STORED` навсегда.
Это конечное состояние, а не зависание: задач нет, `READY` не наступит.

**3. Получить файл.** Два способа, выбор описан в [`API.md`](API.md):

```bash
# presigned-ссылка, трафик мимо сервиса
$GRPCURL "${AUTH[@]}" -d "{\"media_id\":\"$MEDIA\",\"variant\":\"thumb\"}" \
  localhost:9090 media.v1.MediaService/GetDownloadURL

# поток через сервис
$GRPCURL "${AUTH[@]}" -d "{\"media_id\":\"$MEDIA\",\"variant\":\"\"}" \
  localhost:9090 media.v1.MediaService/DownloadStream \
  | jq -r '.data' | base64 -d > out.jpg
```

Presigned-ссылка не аутентифицируется вовсе: кто угодно скачает по ней файл до
истечения `PRESIGN_TTL`.

**4. Список объектов владельца.**

```bash
$GRPCURL "${AUTH[@]}" -d "{\"owner_id\":\"$OWNER\",\"page_size\":50}" \
  localhost:9090 media.v1.MediaService/ListMediaByOwner
```

Объекты в статусе `deleting` в выдачу не попадают: удаление уже начато, и
показывать их вызывающему нечестно. Объекты в статусе `failed` остаются -
владелец должен видеть, что загрузка не удалась.

> **Опережает main.** Исключение статуса `deleting` приезжает с
> [#91](https://github.com/GO-Masterskaya/media-service/issues/91) (PR #92, в
> ревью). В текущем main такие записи в выдаче остаются.

**5. Удалить.**

```bash
$GRPCURL "${AUTH[@]}" -d "{\"media_id\":\"$MEDIA\"}" \
  localhost:9090 media.v1.MediaService/DeleteMedia
```

Удаление устроено как снятие привязки «media -> владелец». Загрузивший объект
владелец привязывается к нему сразу, в той же транзакции, так что отдельного
шага не нужно. Операция идемпотентна: повтор на уже удалённом вернёт `OK`.
Файл физически исчезает только когда снята последняя привязка.

Этому методу `x-owner-id` нужен **всегда**, независимо от
`STRICT_OWNER_CHECK`: без владельца снимать нечего.

`DeleteByOwner` отключён безусловно и возвращает `UNIMPLEMENTED`, пока не
появится настоящая проверка запроса (#5).

### Клиент на Go через сгенерированные stubs

Stubs лежат в репозитории, генерировать ничего не нужно:

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/metadata"

    mediav1 "github.com/GO-Masterskaya/media-service/proto/media/v1"
)

conn, err := grpc.NewClient("localhost:9090",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
    return err
}
defer func() { _ = conn.Close() }()

client := mediav1.NewMediaServiceClient(conn)

ctx = metadata.AppendToOutgoingContext(ctx,
    "authorization", "Bearer "+token,
    "x-owner-id", ownerID.String(),
)

m, err := client.GetMedia(ctx, &mediav1.GetMediaRequest{MediaId: mediaID})
```

`insecure.NewCredentials()` - только для локальной сети. Наружу - TLS.

Загрузка - client-streaming: сначала `Send` с `UploadRequest_Init`, затем в
цикле `UploadRequest_Chunk`, в конце `CloseAndRecv`.

### Что обязательно сделать перед выкатом

- `GRPC_AUTH_ENABLED=true` и настоящий `GRPC_AUTH_TOKEN`. Сервис не стартует,
  если токен пуст при включённой проверке, - это сделано намеренно.
- Порт 9090 не публиковать наружу. `x-owner-id` сервисом не верифицируется:
  его обязан проставлять доверенный посредник, иначе вызывающий подставит
  чужой UUID и получит чужие файлы.
- `STRICT_OWNER_CHECK=true`, если посредник есть. При `false` отсутствие
  заголовка означает анонимный режим без проверки владельца.
- Бакет MinIO приватный, `MINIO_USE_SSL=true`, свои ключи.
- `sslmode` в `POSTGRES_DSN` не `disable`.
- Первый выкат reaper и reconciler - с `TTL_REAP_DRY_RUN=true` и
  `RECONCILER_DRY_RUN=true`: оба удаляют данные безвозвратно, и сначала стоит
  посмотреть на объём в логах.
- Заполнить `CALLER_ID_ALLOWLIST` и научить клиентов слать `x-caller-id`. Без
  этого `RATE_LIMIT_RPS` и `MAX_CONCURRENT_STREAMS` работают как **один общий
  лимит на всех**, а не как лимит на вызывающего, и один шумный клиент выедает
  его целиком.
- `HTTP_ADDR` не публиковать: `/metrics` отдаётся без аутентификации и
  раскрывает имена методов, коды ошибок и профиль нагрузки.

> **Опережает main.** Два последних пункта, `/metrics` и вся тема лимитов,
> приезжают с [#21](https://github.com/GO-Masterskaya/media-service/issues/21)
> (PR #95, без ревью, конфликт в `go.mod`).

---

## Путь 2: встраиваемая библиотека

`pkg/mediaservice` даёт те же операции прямым вызовом, без gRPC. Внутренние
типы наружу не протекают: публичны только структуры библиотеки и её сентинельные
ошибки.

> **Опережает main.** В `go.mod` сейчас объявлен `module mediaservice`, поэтому
> импорты вида `github.com/GO-Masterskaya/media-service/...` в этом документе
> и в примерах ниже станут рабочими только после закрытия
> [#93](https://github.com/GO-Masterskaya/media-service/issues/93)
> (переименование модуля). До тех пор подставляйте `mediaservice/...` либо
> добавьте `replace mediaservice => ../media-service` в свой `go.mod`.
>
> Переименование обязательно: путь `mediaservice` не резолвится как адрес
> репозитория, и `go get` на него не работает - то есть снаружи библиотека
> сейчас не подключается вовсе. Это же касается и `proto/media/v1` из примера
> выше.

### Минимальное подключение

```go
import (
    "context"
    "errors"
    "os"

    "github.com/GO-Masterskaya/media-service/pkg/mediaservice"
)

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

f, err := os.Open("photo.jpg")
if err != nil {
    return err
}
defer func() { _ = f.Close() }()

res, err := client.Upload(ctx, mediaservice.UploadParams{
    OwnerID:        ownerID,
    Filename:       "photo.jpg",
    MIMEType:       "image/jpeg",
    IdempotencyKey: key,
}, f)
if errors.Is(err, mediaservice.ErrAlreadyExists) {
    // ключ переиспользован с другими параметрами
}
```

Готовые компилируемые примеры на все основные методы - в
`pkg/mediaservice/example_test.go`; они же видны в `go doc`.

### Свои соединения

Если пул Postgres и клиент MinIO у приложения уже есть, передайте их через
`NewWithDeps`. `Close()` их **не** тронет - закрывается только то, что
библиотека создала сама:

```go
client, err := mediaservice.NewWithDeps(ctx, mediaservice.Deps{
    Pool: pool, MinIO: mc, Bucket: "media",
})
```

### Схема БД

Три варианта, взаимоисключающих:

1. `WithAutoMigrate()` - библиотека накатывает миграции при создании клиента.
2. `mediaservice.Migrate(dsn)` - явный вызов из кода приложения.
3. `mediaservice.Migrations()` отдаёт `fs.FS` с файлами, и схемой управляет
   ваш мигратор. Пример с `golang-migrate` - в `example_test.go`.

Третий вариант правильный для проектов, где миграции всех компонентов идут
одним потоком.

### Обработка

Движок обработки выключен по умолчанию. Включается `WithProcessing()` или
`WithProcessingConfig(cfg)` - второй включает движок сам, отдельно вызывать
первый не нужно.

Важные следствия встроенного режима:

- **ffmpeg и ffprobe нужны рядом с вашим бинарём.** `ffprobe` требуется
  **всегда**, даже без движка: `Upload` определяет им вид файла, и его
  отсутствие делает любую загрузку невозможной.
- Транскодирование съедает CPU вашего процесса.
- Без включённого движка флаги `Processing` в `UploadParams` игнорируются
  молча: объект останется в статусе `Stored`, производные не появятся.
- Движок живёт под собственным контекстом, не под контекстом конструктора.
  Останавливает его `Close()`.

### Чего библиотека не делает

- **Reaper и reconciler не запускаются.** `TTL` в `UploadParams` запишет момент
  истечения в базу, но удалять просроченное будет некому. Сверки БД и хранилища
  тоже не будет. Если нужны и то и другое - рядом должен работать сервис.
- Нет ни аутентификации, ни rate limiting: библиотека вызывается из вашего
  кода, и решать, кому можно, - ваша задача.

### Ошибки

Типизированные, пригодны для `errors.Is`: `ErrNotFound`, `ErrInvalidArgument`,
`ErrNotReady`, `ErrAccessDenied`, `ErrAlreadyExists`, `ErrQuotaExceeded`,
`ErrStorageFull`, `ErrClosed`, `ErrInternal`. Отмена и таймаут приходят как
обычные `context.Canceled` и `context.DeadlineExceeded`.

`ErrStorageFull` отделена от `ErrInternal` намеренно: `ErrInternal` означает
«сломано, повтор не поможет», а нехватка места временна и повтор позже имеет
смысл.

Соответствие библиотечных ошибок gRPC-кодам однозначное: `ErrNotFound` -
`NOT_FOUND`, `ErrNotReady` - `FAILED_PRECONDITION`, `ErrQuotaExceeded` и
`ErrStorageFull` - `RESOURCE_EXHAUSTED`, и так далее. Таблица кодов - в
[`API.md`](API.md).

---

## Kafka (опционально)

Consumer стартует только при `KAFKA_ENABLED=true` и только в режиме сервиса.
Локально поднимается отдельным профилем:

```bash
make up-kafka
```

Формат сообщения - конверт с вложенным payload. **Обратите внимание:
`SPEC.md` описывает устаревший плоский формат вида `{"type":"detach",...}` -
код его не принимает.**

```json
{
  "event_id": "3f1c...",
  "event_type": "media.attach",
  "timestamp": "2026-09-13T10:00:00Z",
  "payload": { "media_id": "6a2b...", "owner_id": "7c3d..." }
}
```

Поддерживаются ровно два типа: `media.attach` создаёт привязку,
`media.detach` снимает её (та же логика, что у `DeleteMedia`). Любой другой
`event_type` считается неисправимой ошибкой и уезжает в DLQ.

Идемпотентность - по `event_id` через таблицу `processed_events`. Повторная
доставка того же события пропускается. Тот же `event_id` с другим телом -
конфликт отпечатка и DLQ: так ловится переиспользование идентификатора
отправителем.

После исчерпания попыток событие публикуется в `KAFKA_DLQ_TOPIC` с заголовками
`dlq_reason` и `dlq_timestamp`, а offset основного топика коммитится - поток не
встаёт. Разбор накопившегося DLQ - в [`RUNBOOKS.md`](RUNBOOKS.md).

Вне локальной установки: креды задаются только парой `KAFKA_USERNAME` и
`KAFKA_PASSWORD`, и при заданных кредах `KAFKA_TLS=true` обязателен - валидатор
не даст стартовать иначе, потому что SASL/SCRAM трафик не шифрует.

---

## Диагностика первого запуска

**Процесс сразу выходит, в логе имя переменной.** Не прошла валидация конфига.
Имя переменной всегда в тексте ошибки.

**`GRPC_AUTH_TOKEN is required when GRPC_AUTH_ENABLED=true`.** Дефолта у токена
нет намеренно. Задать свой или выключить проверку.

**`UNAUTHENTICATED` на каждый вызов.** Нет заголовка `authorization` либо токен
не тот. Health-методы проверку минуют, так что если они отвечают, а остальное
нет - дело именно в токене.

**`INVALID_ARGUMENT: caller_id required` на `DeleteMedia`.** Этому методу
`x-owner-id` нужен всегда, независимо от `STRICT_OWNER_CHECK`.

**`INVALID_ARGUMENT: corrupt or unreadable media`.** `ffprobe` не разобрал
файл. В режиме библиотеки первым делом проверить, что ffmpeg вообще установлен.

**`Upload` падает по `DEADLINE_EXCEEDED` на большом файле.** Пауза между
сообщениями превысила `UPLOAD_IDLE_TIMEOUT`, по умолчанию 30 секунд.

**`ALREADY_EXISTS` на честном повторе.** Ключ идемпотентности использован с
другими параметрами. Если в запросе есть `ttl` - см. ловушку в
[`API.md`](API.md): повтор с `ttl` конфликтует всегда.

**`RESOURCE_EXHAUSTED: rate limit exceeded` при небольшой нагрузке.** Ключ
лимита - `x-caller-id`, и всё, чего нет в `CALLER_ID_ALLOWLIST`, делит одно
общее ведро. См. [`RUNBOOKS.md`](RUNBOOKS.md), runbook 5.

**`RESOURCE_EXHAUSTED: rate limit exceeded` при небольшой нагрузке.** Скорее
всего вызывающий попал в общее ведро: его `x-caller-id` отсутствует или не
перечислен в `CALLER_ID_ALLOWLIST`. Подробности в [`RUNBOOKS.md`](RUNBOOKS.md).

**`/readyz` отвечает `503`.** Сервис в drain либо Postgres не отвечает на ping.

**`grpcurl` не может разрешить импорт.** Запущен без `-protoset`, см.
«Подготовка grpcurl» выше.