-- Idempotent: на стендах, где 000003 уже прокатана, индекс уже существует —
-- IF NOT EXISTS делает это no-op. На стендах, где 000003 ещё не применена,
-- создаёт индекс без CONCURRENTLY (совместимо с транзакционными миграторами)
CREATE INDEX IF NOT EXISTS idx_media_status_updated_at ON media(status, updated_at);