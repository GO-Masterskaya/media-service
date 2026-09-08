CREATE TYPE event_status AS ENUM
    ('processing', 'done', 'dlq');

CREATE TABLE processed_events (
    event_id uuid NOT NULL,
    fingerprint text NOT NULL,
    status event_status DEFAULT 'processing'::event_status NOT NULL,
    result jsonb,
    owner text NOT NULL,
    lease_expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT processed_events_pkey PRIMARY KEY (event_id)
);

-- Для перехвата просроченного lease и мониторинга зависших событий
CREATE INDEX idx_processed_events_status_lease
    ON processed_events (status, lease_expires_at);

-- Частичный индекс под retention: чистка ходит только по терминальным
-- статусам, поэтому индексировать processing-строки смысла нет.
CREATE INDEX idx_processed_events_retention
    ON processed_events (created_at)
    WHERE status IN ('done', 'dlq');

-- используется существующая  функция set_updated_up() из 000001_init_schema
CREATE TRIGGER trg_processed_events_set_updated_at
BEFORE UPDATE ON processed_events
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
