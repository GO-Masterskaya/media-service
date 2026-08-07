# Бэклог: 29 задач

Основание: [ТЗ](TZ.md), [SPEC](SPEC.md). Номера разделов совпадают с номерами
GitHub Issues в `GO-Masterskaya/media-service`.

## Как устроен бэклог

Задачи разделены по ответственности и имеют проверяемый результат. Крупные
области gRPC, upload, delete, processing и Kafka нарезаны так, чтобы lifecycle,
персистентность и бизнес-обработка не были спрятаны в одной issue.

Волны:

1. **infra/core** (#1–#8, #21, #29) — каркас, инфраструктура, контракт и общие
   адаптеры.
2. **feature** (#9–#18, #22–#28) — пользовательские сценарии и надёжность.
3. **qa** (#19–#20) — сквозная приёмка и инструкция подключения.

Общее для всех задач с кодом:

- `make build`, `make lint`, `make test` зелёные.
- Новая логика покрыта unit-тестами; Postgres, MinIO и Kafka — интеграционными
  тестами там, где они участвуют в сценарии.
- Секреты читаются из env и не попадают в логи.
- Внешний контракт меняется только через `proto/media/v1`.

Способов интеграции два, оба поддерживаемые: вызов по gRPC (контракт из #4) и
встраивание сервиса в чужое Go-приложение как библиотеки (#29, SPEC §2.1).

---

## INFRA

### 1. Каркас Go-модуля, линтер и CI
`infra` `size:M`

**Что делать.** Инициализировать `go.mod` (`module mediaservice`, Go 1.22+),
минимальный `cmd/mediaservice/main.go`, линтер и CI на сборку, `go vet`, линтер
и тесты каждого PR.

**Трогает.** `go.mod`, `cmd/mediaservice`, `.golangci.yml`, `.github/workflows`.

**Зависимости.** Нет.

**Критерии приёмки.**

- `make build`, `make lint`, `make test` работают на чистом клоне.
- CI запускается на PR и падает при ошибке сборки, линтера или теста.
- Версия Go одинакова в `go.mod` и CI.

### 2. Инфраструктура в docker compose
`infra` `size:M`

**Что делать.** Поднять Postgres, MinIO и сервис через Docker Compose; включить
ffmpeg/ffprobe в образ и автоматическое создание private bucket. Подготовить
рабочий `.env.example`.

**Трогает.** `docker-compose.yml`, `Dockerfile`, `.env.example`.

**Зависимости.** #1.

**Критерии приёмки.**

- `cp .env.example .env && make up` поднимает окружение на чистой машине.
- В контейнере доступны `ffmpeg` и `ffprobe`; bucket создаётся автоматически.
- Postgres и MinIO имеют healthcheck, сервис ждёт их готовности.
- Порты Postgres и MinIO не публикуются в prod-профиле.

### 3. Конфиг, логирование, graceful shutdown процесса
`infra` `size:M`

**Что делать.** Загружать и валидировать env-конфиг, настроить структурные
логи `slog` и общий lifecycle процесса с контекстом и shutdown timeout.
Компонентные процедуры остановки processing и Kafka остаются в #26 и #27.

**Трогает.** `internal/config`, `cmd/mediaservice`.

**Зависимости.** #1.

**Критерии приёмки.**

- Все параметры SPEC §7 читаются из env; ошибка на обязательном параметре
  содержит его имя.
- Секреты не выводятся в логах.
- Сигнал завершения отменяет общий контекст и соблюдает `SHUTDOWN_TIMEOUT`.
- Тест процесса не обнаруживает утечек горутин.

---

## CORE

### 4. gRPC-контракт и кодогенерация
`core` `size:M`

**Что делать.** Описать `MediaService` и сообщения по SPEC §3, правила
валидации и воспроизводимую генерацию Go stubs через `make proto`.

**Трогает.** `proto/media/v1`, `Makefile`, `README.md`.

**Зависимости.** #1.

**Критерии приёмки.**

- В proto есть все RPC из SPEC, включая client- и server-streaming.
- `make proto` воспроизводим и повторный запуск не создаёт diff.
- Невалидные обязательные поля отклоняются до бизнес-логики.
- README объясняет установку toolchain и использование сгенерированных stubs.

### 5. gRPC runtime: lifecycle, health и базовые interceptors
`core` `size:M`

**Что делать.** Поднять gRPC-сервер, зарегистрировать сервис и стандартный
health-check, реализовать readiness/liveness и graceful stop. Собрать базовую
цепочку interceptors: panic recovery, correlation-id и чтение API-token из
metadata без валидации в v1. Метрики и лимиты вынесены в #21.

**Трогает.** `internal/api` (runtime), `cmd/mediaservice`.

**Зависимости.** #1, #3, #4.

**Критерии приёмки.**

- Сервер стартует; gRPC health и HTTP-пробы корректно отражают готовность.
- Паника handler не роняет процесс и возвращает `INTERNAL`.
- Входящий correlation-id попадает в контекст и лог; отсутствующий генерируется.
- API-token читается и прокидывается, но намеренно не валидируется.
- Graceful stop прекращает приём новых RPC и ждёт активные стримы в пределах
  общего timeout.

### 6. Базовая схема БД, миграции и пул подключений
`core` `size:M`

**Что делать.** Создать базовые таблицы `media`, `media_derivative`,
`processing_jobs`, индексы и ограничения по SPEC §6; настроить `pgxpool` и
запуск миграций. Детальные job-переходы и ограничения — #25, журнал Kafka — #28.

**Трогает.** `migrations`, `internal/repo` (подключение и общие helpers).

**Зависимости.** #1, #3.

**Критерии приёмки.**

- Миграции проходят на чистой БД и повторный запуск безопасен.
- Есть базовые таблицы/индексы из SPEC и unique `(owner_id, idempotency_key)`.
- Пул имеет конфигурируемые таймауты подключения и запроса.
- Схема меняется только новыми миграциями; правило записано в README.

### 7. Адаптер объектного хранилища MinIO
`core` `size:M`

**Что делать.** Реализовать потоковые put/get, presign, удаление объекта и
префикса. Ключи строятся по layout SPEC от owner/media id, без сырого filename.

**Трогает.** `internal/storage`.

**Зависимости.** #1, #3.

**Критерии приёмки.**

- Интеграционные тесты покрывают put/get/presign/delete.
- Файл не загружается целиком в память.
- Presigned URL истекает по TTL; удаление отсутствующего объекта идемпотентно.
- Пути и спецсимволы в filename не влияют на storage key.

### 8. Обёртка ffprobe и ffmpeg
`core` `size:L`

**Что делать.** Безопасно запускать ffprobe/ffmpeg через `exec` без shell,
извлекать метаданные и поддержать операции preview/transcode с timeout и
отменой дочернего процесса.

**Трогает.** `internal/processing` (media tools, без engine).

**Зависимости.** #1.

**Критерии приёмки.**

- Фикстуры image/video/audio дают ожидаемый kind и metadata.
- Поддельное расширение определяется по содержимому; битый файл даёт ошибку.
- Отмена убивает дочерний процесс без zombie.
- Пользовательские строки не интерполируются в shell-команду.

### 21. gRPC observability и per-caller limits
`core` `size:M`

**Что делать.** Добавить interceptor-слой observability и защиты: request
logging без payload/секретов, Prometheus-метрики RPC и потоков, rate limit и
лимит одновременных upload/download streams по стабильному caller identity.
Определить fallback identity и bounded cleanup состояния неактивных callers.

**Трогает.** `internal/api` (interceptors), `internal/metrics`, config.

**Зависимости.** #3, #5.

**Критерии приёмки.**

- Метрики содержат count, duration, gRPC code и активные streams без
  high-cardinality labels (`owner_id`, token, media id запрещены).
- Превышение RPS или concurrent streams даёт `RESOURCE_EXHAUSTED` только
  виновному caller; другой caller не затронут.
- Unary и streaming RPC используют одну документированную схему caller identity.
- Отмена/ошибка стрима всегда освобождает слот; состояние ушедших callers
  ограничено по памяти и очищается.
- `/metrics`, `/livez`, `/readyz` доступны на отдельном HTTP-порту.

### 29. Встраиваемая Go-библиотека `pkg/mediaservice`
`core` `size:L`

**Что делать.** Дать второй способ интеграции (SPEC §2.1): подключение сервиса
в чужое Go-приложение зависимостью, без поднятия gRPC. Библиотека сама работает
с Postgres и MinIO и предоставляет методы всего жизненного цикла медиа.

Публичный контракт в `pkg/mediaservice`:

- Конструктор в двух вариантах: по параметрам подключения (DSN Postgres,
  реквизиты MinIO) и по **уже готовым** `*pgxpool.Pool` и клиенту MinIO — чтобы
  встраивающий проект переиспользовал свои соединения.
- Методы: загрузка из `io.Reader`, получение метаданных, список по владельцу с
  пагинацией, presigned-ссылка, открытие содержимого как `io.ReadCloser`,
  удаление медиа и удаление по владельцу.
- Включение асинхронной обработки опцией; без неё библиотека полностью рабочая.
- Публичные типы и ошибки объявлены в `pkg/mediaservice`, типы из `internal/`
  наружу не протекают.
- Миграции: библиотека умеет применить схему сама и умеет отдать миграции
  наружу, если встраивающий проект применяет их своим мигратором.
- `Close()` освобождает только созданные библиотекой ресурсы; переданные извне
  пул и клиент не закрываются.

**Ключевое ограничение.** Доменная логика не дублируется: библиотека и
gRPC-хендлеры вызывают одно и то же ядро из `internal/media`. `pkg/mediaservice`
— тонкая публичная обёртка с маппингом типов, а не вторая реализация. Появление
второй копии логики загрузки или удаления — повод развернуть ревью.

**Трогает.** `pkg/mediaservice` (новый пакет), точечно `internal/media` — если
понадобится выделить конструкторы ядра, пригодные к переиспользованию.

**Зависимости.** Ядро домена: #6, #7 и реализованные сценарии #9–#13.
Интерфейс библиотеки фиксируется в начале задачи и дальше не меняется, чтобы
сценарии могли доезжать в него по мере готовности.

**Критерии приёмки.**

- Отдельный тестовый Go-модуль импортирует `pkg/mediaservice` и проходит полный
  цикл: загрузка файла, чтение метаданных, получение ссылки, чтение содержимого
  потоком, удаление — без запуска gRPC-сервера.
- Тест: конструктор принимает готовые `*pgxpool.Pool` и клиент MinIO, и после
  `Close()` переданные извне соединения остаются рабочими.
- Тест: библиотека применяет миграции на чистой базе; вариант с внешним
  мигратором тоже поддержан и покрыт тестом.
- Тест: загрузка и чтение идут потоком, память не растёт пропорционально
  размеру файла.
- Тест: с выключенной обработкой библиотека рабочая, медиа доходит до `stored`,
  задачи обработки не создаются.
- Тест: с включённой обработкой производные создаются, медиа доходит до `ready`.
- Ошибки библиотеки — публичные типизированные значения, пригодные для
  `errors.Is`/`errors.As`; внутренние типы в них не протекают (проверяется
  тестом и ревью).
- В README есть рабочий пример подключения библиотекой на 15–20 строк.
- `go vet` и линтер не ругаются на экспортируемый API, публичные типы имеют
  комментарии.

---

## FEATURE

### 9. Upload RPC: протокол стрима, валидация и media inspection
`feature` `size:M`

**Что делать.** Реализовать client-streaming протокол: `UploadInit` строго
первым, затем chunks; проверить UUID, mime allowlist, declared/actual size,
пропустить временный файл через ffprobe и отклонить mime-class mismatch.
Временными файлами владеет #22, атомарной записью upload — #23.

**Трогает.** `internal/api` (Upload), `internal/media` (validation/orchestration).

**Зависимости.** #4, #5, #7, #8, #22, #23.

**Критерии приёмки.**

- Неверный порядок сообщений, лишний init и chunk до init дают
  `INVALID_ARGUMENT` без side effects.
- UUID, mime, expected size и фактический лимит проверяются.
- Поддельный mime-class и битый файл отклоняются до постоянной записи.
- Upload большого файла имеет bounded memory usage.
- Ошибки корректно маппятся в gRPC codes из SPEC.

### 10. Чтение метаданных и список по владельцу
`feature` `size:M`

**Что делать.** Реализовать `GetMedia` и `ListMediaByOwner` с производными,
статусом и keyset-пагинацией по `(created_at, id)`.

**Трогает.** `internal/api`, `internal/media`, `internal/repo`.

**Зависимости.** #4–#7.

**Критерии приёмки.**

- `GetMedia` возвращает metadata/derivatives или `NOT_FOUND`.
- Страницы списка стабильны, не пересекаются и не теряют записи при вставках.
- Данные разных owners не смешиваются.

### 11. Ссылка на скачивание (presigned URL)
`feature` `size:M`

**Что делать.** Реализовать `GetDownloadURL` для `original`, `thumbnail`,
`r_720`, проверяя наличие варианта и готовность производной.

**Трогает.** `internal/api`, `internal/media`, `internal/repo`, `internal/storage`.

**Зависимости.** #4–#7.

**Критерии приёмки.**

- URL скачивает верный объект и перестаёт работать после TTL.
- Отсутствующий/неготовый вариант даёт `NOT_FOUND`/`FAILED_PRECONDITION`, а
  не пустую ссылку.

### 12. Отдача файла потоком (server-streaming)
`feature` `size:M`

**Что делать.** Реализовать `DownloadStream` с конфигурируемыми chunks,
backpressure и освобождением ресурсов при отмене клиента.

**Трогает.** `internal/api`, `internal/media`, `internal/storage`.

**Зависимости.** #4–#7, #21.

**Критерии приёмки.**

- Результат побайтно совпадает с объектом; файл не читается целиком в RAM.
- Отмена клиента закрывает reader и освобождает stream slot без утечки.
- Отсутствующий media/variant отклоняется до начала ответа.

### 13. DeleteMedia и DeleteByOwner: доменная команда
`feature` `size:M`

**Что делать.** Реализовать идемпотентные hard-delete команды: атомарно
пометить записи `deleting`, удалить известные original/derivatives из MinIO,
после успеха удалить строки БД. Реализовать безопасный batch `DeleteByOwner`.
Фоновая сверка вынесена в #24.

**Трогает.** `internal/api`, `internal/media`, `internal/repo`, `internal/storage`.

**Зависимости.** #4–#7.

**Критерии приёмки.**

- После успешного delete нет известных строк и объектов.
- Повторный delete отсутствующего media возвращает успех.
- Ошибка MinIO оставляет запись `deleting`, доступную для reconciliation.
- `DeleteByOwner` удаляет только целевого owner, работает батчами и сообщает
  детерминированный `deleted_count`.

### 14. Processing engine: dispatcher, channel и worker pool
`feature` `size:M`

**Что делать.** Реализовать bounded `jobCh`, feeder и пул workers с
`WORKER_CONCURRENCY`, backpressure и registry обработчиков. Engine опирается
на repository/state machine #25; recovery/retries/shutdown вынесены в #26.

**Трогает.** `internal/processing` (engine core).

**Зависимости.** #3, #25.

**Критерии приёмки.**

- Одновременно работает не больше `WORKER_CONCURRENCY` jobs.
- Feeder claim-ит не больше свободной ёмкости; переполнение channel не теряет jobs.
- Handler registry различает типы jobs и отвергает неизвестный тип управляемой
  ошибкой.
- Метрики отражают channel depth, DB queue depth и in-flight workers.

### 15. Генерация превью
`feature` `size:M`

**Что делать.** Handler thumbnail/preview: video frame, audio waveform, image
thumbnail; загрузить производную в MinIO и зарегистрировать через repository.

**Трогает.** `internal/processing`, `internal/repo`, `internal/storage`.

**Зависимости.** #7, #8, #14, #25.

**Критерии приёмки.**

- Фикстуры создают производную нужного формата и metadata.
- Повторная/конкурентная обработка одного `(media_id, variant)` не создаёт
  дубликаты строк или объектов и возвращает тот же результат.
- Ошибка после загрузки объекта, но до DB commit, восстанавливается повтором
  без orphan derivative.
- Битый source завершает job управляемой ошибкой; параметры берутся из config.

### 16. Транскодинг и нормализация
`feature` `size:L`

**Что делать.** Handler transcode: video → 720p h264/aac, audio normalization,
image resize; сохранить производную через идемпотентный repository. Решение о
переходе media в `ready` принадлежит state machine #25, не handler.

**Трогает.** `internal/processing`, `internal/repo`, `internal/storage`.

**Зависимости.** #7, #8, #14, #25.

**Критерии приёмки.**

- Форматы и кодеки результата подтверждаются ffprobe.
- Повторные/конкурентные jobs одного типа не создают duplicate derivative или
  лишний MinIO object.
- Завершение одного job не переводит media в `ready`, пока другой обязательный
  job ещё `queued/running`.
- Ошибка ffmpeg сохраняет причину и не роняет процесс.

### 17. Время жизни медиа и TTL reaper
`feature` `size:M`

**Что делать.** Хранить `expires_at` и удалять истёкшие media батчами через
ту же доменную команду delete; без TTL хранить бессрочно.

**Трогает.** `internal/media`, `internal/repo`.

**Зависимости.** #13, #24.

**Критерии приёмки.**

- Истёкшее media удаляется вместе со всеми производными; неистёкшее и без TTL
  не затрагиваются.
- Ошибка одной записи не останавливает batch.
- Период и batch size конфигурируются; параллельные reapers не удаляют дважды.

### 18. Kafka event handling: attach, detach и DLQ policy
`feature` `size:M`

**Что делать.** Валидировать JSON envelope и обрабатывать `attach`/`detach`
через доменные команды. Классифицировать retryable/permanent ошибки и после
исчерпания event retries публиковать DLQ с причиной. Kafka lifecycle — #27,
идемпотентный journal — #28.

**Трогает.** `internal/events` (decoder/handler), `internal/media`.

**Зависимости.** #13, #27, #28.

**Критерии приёмки.**

- Валидный `detach` использует DeleteMedia; `attach` проверяет/проставляет owner.
- Невалидный JSON/schema и permanent error идут в DLQ с исходным `event_id`
  и причиной; следующие события продолжают обрабатываться.
- Offset подтверждается только после атомарно зафиксированного результата или DLQ.
- Повтор события использует результат #28 и не повторяет side effect.

### 22. Upload temp storage: lifecycle, cleanup и ENOSPC
`feature` `size:M`

**Что делать.** Выделить менеджер временного upload storage: безопасная
директория/имена, streaming write с hard limit, quota/reserved-space policy,
cleanup на всех terminal paths и уборка stale files после рестарта.

**Трогает.** `internal/media` или `internal/upload` (temp storage), config.

**Зависимости.** #1, #3.

**Критерии приёмки.**

- Success, validation error, client cancel, panic и timeout удаляют temp file.
- Превышение фактического/declared limit прекращает запись и чистит partial file.
- `ENOSPC`/quota exhaustion возвращается как `RESOURCE_EXHAUSTED`, не приводит
  к MinIO/DB side effects и не ломает следующий upload.
- Startup cleanup удаляет только stale-файлы сервиса старше grace period и не
  затрагивает активные uploads.
- Метрики показывают temp bytes/files и cleanup failures.

### 23. Upload persistence и строгая idempotency
`feature` `size:L`

**Что делать.** Спроектировать атомарную персистентность upload между MinIO,
`media` и enqueue requested jobs. Сохранить fingerprint тела/существенных
параметров для строгой семантики `(owner_id, idempotency_key)` и компенсировать
частичные сбои.

**Трогает.** `internal/media`, `internal/repo`, `internal/storage`, migrations.

**Зависимости.** #6, #7, #25.

**Критерии приёмки.**

- Первый upload создаёт ровно один media и требуемый набор jobs.
- Повтор того же owner/key с тем же телом и существенными параметрами возвращает
  исходный `media_id`, не создавая object/row/job.
- Тот же owner/key с другим body fingerprint либо параметрами возвращает
  `ALREADY_EXISTS`; конфликт проверяется и при конкурентных uploads.
- Сбой между MinIO put и DB commit не оставляет необнаружимый orphan; повтор
  безопасно завершает или компенсирует операцию.
- DB commit media + jobs атомарен; `processing` не виден без jobs, `stored` не
  содержит незаявленных jobs.

### 24. Delete reconciliation и orphan MinIO objects
`feature` `size:M`

**Что делать.** Реализовать периодический reconciler для записей `deleting`
старше grace period и orphan objects в service-owned MinIO prefix. Определить
checkpoint/batch, retry/backoff, ownership guard и наблюдаемость.

**Трогает.** `internal/media` (reconciler), `internal/repo`, `internal/storage`.

**Зависимости.** #7, #13.

**Критерии приёмки.**

- Зависшая `deleting` запись после временного сбоя доводится до полного удаления.
- Объект без DB owner старше grace period удаляется; новый/in-flight object не
  удаляется.
- Reconciler не удаляет вне выделенного service prefix/bucket и проверяет
  ownership перед каждым destructive action.
- Падение/рестарт посреди batch безопасны; операция идемпотентна.
- Есть метрики scanned/deleted/failed/orphans и структурные логи причин.

### 25. Processing jobs repository, state machine и deduplication
`feature` `size:L`

**Что делать.** Реализовать repository и формальную state machine
`queued → running → done|failed` с допустимым `running → queued`. Добавить
атомарный claim через `FOR UPDATE SKIP LOCKED`, ownership/lease проверки,
dedup jobs и derivatives, атомарный пересчёт статуса media.

**Трогает.** `internal/repo`, migrations, domain job model.

**Зависимости.** #6.

**Критерии приёмки.**

- Параллельные claimers не получают один job; завершить job может только его
  текущий owner/lease.
- Unique policy не допускает более одного активного/логического job на
  `(media_id, type)` и более одной derivative на `(media_id, variant)`.
- Enqueue одной processing intent повторно/конкурентно возвращает существующий job.
- `MarkDone` и переход media выполняются в одной DB transaction: media становится
  `ready` только если все требуемые jobs `done`; конкурентные последние jobs не
  дают lost update или преждевременный `ready`.
- Исчерпавшийся обязательный job атомарно переводит media в `failed` с причиной;
  недопустимые переходы отвергаются и тестируются.

### 26. Processing engine: retries, recovery и graceful shutdown
`feature` `size:M`

**Что делать.** Поверх #14/#25 реализовать exponential backoff с jitter и max
attempts, startup/stale-lease recovery, per-job timeout и graceful drain с
возвратом незавершённых jobs в `queued`.

**Трогает.** `internal/processing`, `internal/repo`, config.

**Зависимости.** #3, #14, #25.

**Критерии приёмки.**

- Retryable failure увеличивает attempts и задаёт `run_after`; permanent/max
  attempts завершает job/media как `failed`.
- После crash только stale `running` jobs возвращаются в `queued`; живой lease
  другого instance не перехватывается.
- Shutdown сначала останавливает feeder, затем дренит workers до timeout;
  незавершённые jobs становятся доступными для retry и не теряются.
- Job timeout убивает ffmpeg child; повторный старт не создаёт duplicate
  derivative благодаря #25.
- Метрики покрывают retries, recoveries, processing duration и shutdown outcome.

### 27. Kafka lifecycle и test infrastructure
`feature` `size:M`

**Что делать.** Выделить создание Kafka client/consumer group, конфиг,
подписку, poll loop, manual commit, producer DLQ и shutdown. Добавить Kafka в
dev/test compose или testcontainers. Не реализовывать attach/detach здесь.

**Трогает.** `internal/events` (runtime), config, compose/test infrastructure.

**Зависимости.** #2, #3.

**Критерии приёмки.**

- При `KAFKA_ENABLED=false` Kafka client не создаётся и сервис полностью готов.
- При `true` consumer group стартует после readiness зависимостей, reconnect
  имеет bounded backoff, credentials не логируются.
- Offset commit только manual; runtime предоставляет handler-у явный ack/DLQ
  результат.
- Shutdown прекращает poll, дожидается in-flight handler в пределах timeout и
  закрывает consumer/producer без утечек.
- Интеграционный fixture создаёт input/DLQ topics и пригоден для CI.

### 28. Kafka processed_events: migrations и idempotency ownership
`feature` `size:M`

**Что делать.** Добавить `processed_events` и repository протокол ownership:
atomic claim по `event_id`, payload fingerprint, состояния processing/done/DLQ,
lease/recovery и фиксацию результата до offset commit.

**Трогает.** migrations, `internal/repo` или `internal/events/repo`.

**Зависимости.** #6.

**Критерии приёмки.**

- Миграция задаёт unique `event_id`, status/result, fingerprint, owner/lease и
  нужные индексы; rollback/forward проверены.
- Два consumers одного `event_id` не выполняют side effect параллельно.
- Повтор done/DLQ event возвращает сохранённый результат; тот же `event_id` с
  другим payload fingerprint считается конфликтом и не исполняется.
- Crash после domain commit, но до offset commit восстанавливается без повторного
  side effect.
- Stale processing claim можно безопасно забрать после lease expiry; живой — нельзя.

---

## QA

### 19. Приёмочные интеграционные тесты
`qa` `size:L`

**Что делать.** Собрать сквозной suite по ТЗ §7 и критериям issues #21–#28 на
testcontainers; включить его в CI.

**Трогает.** Пакет acceptance/integration tests, CI.

**Зависимости.** Реализованные feature issues.

**Критерии приёмки.**

- Полный путь upload → processing → ready → download → delete проходит.
- Отдельно покрыты concurrent idempotency conflict, temp `ENOSPC`, delete
  reconciliation/orphans, crash recovery, atomic ready/dedup и Kafka replay.
- Все пункты ТЗ §7 автоматизированы либо имеют обоснованную manual-проверку.
- Десять последовательных прогонов не дают flaky failures.

### 20. Инструкция по подключению сервиса
`qa` `size:M`

**Что делать.** Описать запуск сервиса, env, gRPC contract, streaming upload и
download, error semantics, limits, processing/Kafka lifecycle и операционные
runbooks. Примеры используют grpcurl или generated stubs.

**Трогает.** `README.md`, `docs/`.

**Зависимости.** #4 и стабильные feature issues.

**Критерии приёмки.**

- Новый интегратор по инструкции запускает сервис и проходит основной сценарий.
- Все env имеют назначение, default и security note.
- Для каждого RPC есть рабочий пример; описаны gRPC codes, idempotency conflict,
  rate/stream limits, retry guidance и readiness.
- Есть runbooks для ENOSPC, stuck processing, delete reconciliation и Kafka DLQ.
- Описаны оба способа интеграции и когда какой выбирать: вызов по gRPC
  (proto и сгенерированные stubs) и встраивание библиотекой `pkg/mediaservice`
  (#29), со ссылкой на пример подключения.
