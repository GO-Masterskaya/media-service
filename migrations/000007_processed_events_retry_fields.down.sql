ALTER TABLE processed_events
    DROP COLUMN IF EXISTS retry_count,
    DROP COLUMN IF EXISTS last_error_at;