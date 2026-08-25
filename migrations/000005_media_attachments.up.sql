-- +++ UP: media_attachments + usages_count
-- Позволяет одно media прикреплять к нескольким сущностям.

CREATE TABLE IF NOT EXISTS media_attachments (
    media_id UUID NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    owner_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (media_id, owner_id)
);

CREATE INDEX IF NOT EXISTS idx_media_attachments_owner
    ON media_attachments(owner_id);

-- Счётчик живых привязок.
ALTER TABLE media
    ADD COLUMN IF NOT EXISTS usages_count INT NOT NULL DEFAULT 0;

-- Защита от рассинхрона (idempotent).
DO $$
BEGIN
    ALTER TABLE media
        ADD CONSTRAINT chk_media_usages_count_nonnegative
        CHECK (usages_count >= 0);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Backfill: мигрируем существующие owner_id → media_attachments.
-- Считаем реальное количество привязок, не трогаем уже заполненные.
INSERT INTO media_attachments (media_id, owner_id)
SELECT id, owner_id FROM media
WHERE owner_id IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE media m
SET usages_count = COALESCE((SELECT COUNT(*) FROM media_attachments a WHERE a.media_id = m.id), 0)
WHERE m.usages_count = 0;