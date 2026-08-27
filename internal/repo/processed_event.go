package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventStatus string

const (
	EventStatusProcessing EventStatus = "processing"
	EventStatusDone       EventStatus = "done"
	EventStatusDLQ        EventStatus = "dlq"
)

// ProcessedEvent - запись о владении и идемпотентности для одного event_id из Kafka:
// какой consumer сейчас владеет им (или завершил его последним) и каков был
// сохраненный результат, если он есть.
type ProcessedEvent struct {
	EventID        uuid.UUID
	Fingerprint    string
	Status         EventStatus
	Result         []byte
	Owner          string
	LeaseExpiresAt time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	RetryCount     int
	LastErrorAt    *time.Time
}

// ProcessedEventRepo защищает побочный эффект (side-effect) обработчика Kafka-событий с помощью
// атомарного захвата (claim), чтобы конкурентные или передоставленные события
// обрабатывались ровно один раз (или безопасно воспроизводились).
type ProcessedEventRepo interface {
	// Claim атомарно захватывает право владения eventID для обработки.
	//
	// claimed=true: вызывающий теперь владеет lease (это либо совершенно новое
	// событие, либо просроченный lease, который он только что перехватил) и должен
	// выполнить side-effect, а затем вызвать MarkDone/MarkDLQ *до* подтверждения
	// (ack) смещения (offset) в Kafka.
	//
	// claimed=false, err=nil: событие уже достигло терминального состояния
	// (done/dlq) с совпадающим fingerprint. event.Result содержит сохраненный
	// результат; вызывающий не должен повторять side-effect и должен просто
	// подтвердить (ack) offset.
	//
	// Ошибки:
	//   - ErrFingerprintConflict: event_id уже встречался ранее с другим
	//     fingerprint полезной нагрузки. Событие не должно выполняться.
	//   - ErrClaimHeld: другой владелец в настоящее время удерживает активный
	//     (неистекший) processing lease для этого event_id. Вызывающему следует
	//     отступить и дать ему завершиться (или истечь по таймауту).
	Claim(ctx context.Context, eventID uuid.UUID, fingerprint, owner string, lease time.Duration) (event *ProcessedEvent, claimed bool, err error)

	// MarkDone завершает обработку claim со статусом done и надежно сохраняет результат.
	// Должен быть зафиксирован до подтверждения (ack) offset в Kafka, чтобы сбой
	// между этими действиями просто привел к повторной доставке, которую Claim()
	// разрешит через сохраненный результат, вместо повторного выполнения side-effect.
	//
	// Возвращает ErrClaimLost, если владелец больше не удерживает claim
	// (его lease истек и был перехвачен до завершения).
	MarkDone(ctx context.Context, eventID uuid.UUID, owner string, result []byte) error

	// MarkDLQ завершает обработку claim со статусом отправки в топик DLQ,
	// сохраняя причину в качестве результата. Гарантии владения и порядка
	// те же, что и у MarkDone.
	MarkDLQ(ctx context.Context, eventID uuid.UUID, owner, reason string) error

	// DeleteTerminalOlderThan удаляет записи в терминальных статусах
	// (done/dlq), созданные раньше olderThan, и возвращает число удалённых.
	// Записи в статусе processing не трогаются никогда — они ещё могут
	// понадобиться для recovery.
	//
	// Нужен для retention: без периодической чистки таблица растёт вечно.
	// Окно olderThan должно с запасом превышать retention Kafka-топика,
	// иначе можно удалить запись о событии, которое ещё может быть
	// передоставлено, и потерять защиту от повторного side-effect.
	// limit ограничивает размер одной пачки, чтобы не держать долгую
	// блокировку. Вызов из периодической задачи — вайринг в #18/#39.
	DeleteTerminalOlderThan(ctx context.Context, olderThan time.Time, limit int) (int64, error)
	// BumpAttempt атомарно инкрементирует счётчик попыток в result jsonb.
	// Возвращает новое значение. Требует status='processing' и совпадения owner.
	BumpAttempt(ctx context.Context, eventID uuid.UUID, owner string) (int, error)
}

type PgProcessedEventRepo struct {
	pool *pgxpool.Pool
}

func NewPgProcessedEventRepo(pool *pgxpool.Pool) *PgProcessedEventRepo {
	return &PgProcessedEventRepo{pool: pool}
}

func (r *PgProcessedEventRepo) Claim(ctx context.Context, eventID uuid.UUID, fingerprint, owner string, lease time.Duration) (*ProcessedEvent, bool, error) {
	if lease <= 0 {
		return nil, false, fmt.Errorf("repo: lease must be positive")
	}

	// Единый атомарный upsert: захватывает блокировку строки при конфликте и
	// перехватывает claim только если существующий lease находится в статусе
	// processing, истёк И payload тот же. Если условие WHERE исключает
	// обновление, здесь не возвращается ни одной строки, и мы классифицируем
	// существующую строку ниже - это позволяет избежать гонки read-then-write
	// между конкурентными consumer'ами.
	//
	// Проверка fingerprint в WHERE обязательна: без неё событие с тем же
	// event_id, но другим телом молча перехватило бы протухший claim и
	// выполнило side effect. fingerprint в SET не меняется, поэтому RETURNING
	// вернул бы старый и ErrFingerprintConflict не сработал бы вообще.
	const upsert = `
		INSERT INTO processed_events (event_id, fingerprint, status, owner, lease_expires_at, created_at, updated_at, retry_count, last_error_at)
		VALUES ($1, $2, 'processing', $3, now() + make_interval(secs => $4), now(), now(), 0, NULL)
		ON CONFLICT (event_id) DO UPDATE SET
		owner = EXCLUDED.owner,
		fingerprint = EXCLUDED.fingerprint,
		lease_expires_at = NOW() + make_interval(secs => $4),
		updated_at = NOW()
		WHERE processed_events.status = 'processing'
		AND (processed_events.lease_expires_at < NOW()
		OR processed_events.owner = EXCLUDED.owner)
		  AND processed_events.fingerprint = EXCLUDED.fingerprint
		RETURNING event_id, fingerprint, status, result, owner, lease_expires_at, created_at, updated_at, retry_count, last_error_at`

	row := r.pool.QueryRow(ctx, upsert, eventID, fingerprint, owner, lease.Seconds())
	event, err := scanProcessedEvent(row)
	if err == nil {
		return event, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("repo: claim processed event: %w", err)
	}

	existing, getErr := r.getByID(ctx, eventID)
	if getErr != nil {
		return nil, false, fmt.Errorf("repo: claim processed event: inspect existing: %w", getErr)
	}

	if existing.Fingerprint != fingerprint {
		return nil, false, ErrFingerprintConflict
	}

	switch existing.Status {
	case EventStatusDone, EventStatusDLQ:
		return existing, false, nil
	default:
		return nil, false, ErrClaimHeld
	}
}

func (r *PgProcessedEventRepo) MarkDone(ctx context.Context, eventID uuid.UUID, owner string, result []byte) error {
	return r.finalize(ctx, eventID, owner, EventStatusDone, result)
}

func (r *PgProcessedEventRepo) MarkDLQ(ctx context.Context, eventID uuid.UUID, owner, reason string) error {
	result, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return fmt.Errorf("repo: marshal dlq reason: %w", err)
	}
	return r.finalize(ctx, eventID, owner, EventStatusDLQ, result)
}

func (r *PgProcessedEventRepo) finalize(ctx context.Context, eventID uuid.UUID, owner string, status EventStatus, result []byte) error {
	const q = `
		UPDATE processed_events
		SET status = $4::event_status, result = $3, updated_at = now()
		WHERE event_id = $1 AND owner = $2 AND status = 'processing'`

	tag, err := r.pool.Exec(ctx, q, eventID, owner, result, string(status))
	if err != nil {
		return fmt.Errorf("repo: finalize processed event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrClaimLost
	}
	return nil
}

// DeleteTerminalOlderThan удаляет пачку терминальных записей старше olderThan.
func (r *PgProcessedEventRepo) DeleteTerminalOlderThan(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("repo: limit must be positive")
	}

	// Паттерн как в ClaimNext (job.go): CTE отбирает пачку под блокировкой,
	// SKIP LOCKED позволяет нескольким воркерам чистки разбирать
	// непересекающиеся пачки и реально работать параллельно, а не ждать
	// друг друга и возвращать ноль.
	//
	// LIMIT обязателен: без него при долгом простое чистки один DELETE
	// попытался бы снести миллионы строк в одной транзакции - длинная
	// блокировка, распухший WAL и почти гарантированный statement_timeout.
	//
	// Отбор идёт по частичному индексу idx_processed_events_retention
	// (created_at) WHERE status IN ('done','dlq'): предикат совпадает,
	// ORDER BY идёт в порядке индекса, LIMIT даёт ранний выход.
	const q = `
		WITH doomed AS (
			SELECT event_id
			FROM processed_events
			WHERE status IN ('done', 'dlq')
				AND created_at < $1
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		DELETE FROM processed_events p
		USING doomed
		WHERE p.event_id = doomed.event_id`

	tag, err := r.pool.Exec(ctx, q, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("repo: delete terminal processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *PgProcessedEventRepo) getByID(ctx context.Context, eventID uuid.UUID) (*ProcessedEvent, error) {
	const q = `
		SELECT event_id, fingerprint, status, result, owner, lease_expires_at, created_at, updated_at, retry_count, last_error_at
		FROM processed_events
		WHERE event_id = $1`

	ev, err := scanProcessedEvent(r.pool.QueryRow(ctx, q, eventID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("repo: get processed event: %w", err)
	}
	return ev, nil
}

func scanProcessedEvent(row pgx.Row) (*ProcessedEvent, error) {
	var event ProcessedEvent
	if err := row.Scan(
		&event.EventID,
		&event.Fingerprint,
		&event.Status,
		&event.Result,
		&event.Owner,
		&event.LeaseExpiresAt,
		&event.CreatedAt,
		&event.UpdatedAt,
		&event.RetryCount,
		&event.LastErrorAt,
	); err != nil {
		return nil, err
	}
	return &event, nil
}

func (r *PgProcessedEventRepo) BumpAttempt(ctx context.Context, eventID uuid.UUID, owner string) (int, error) {
	const q = `
		UPDATE processed_events
		SET retry_count = retry_count + 1,
+		    last_error_at = NOW(),
 		    updated_at = now()
 		WHERE event_id = $1 AND owner = $2 AND status = 'processing'
		RETURNING retry_count`
	var attempts int
	err := r.pool.QueryRow(ctx, q, eventID, owner).Scan(&attempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrClaimLost
		}
		return 0, fmt.Errorf("repo: bump attempt: %w", err)
	}
	return attempts, nil
}
