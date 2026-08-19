CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_media_status_updated_at ON media(status, updated_at);

