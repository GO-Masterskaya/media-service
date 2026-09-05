CREATE INDEX IF NOT EXISTS idx_media_owner_created_id
ON media (owner_id, created_at DESC, id DESC);
