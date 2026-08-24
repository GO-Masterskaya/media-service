-- +++ ADDED: media_attachments + usages_count
-- Позволяет одно media прикреплять к нескольким сущностям.

CREATE TABLE IF NOT EXISTS media_attachments (
    media_id UUID NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    owner_id UUID NOT NULL,                    -- сущность, к которой прикреплено
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (media_id, owner_id)
);

CREATE INDEX IF NOT EXISTS idx_media_attachments_owner
    ON media_attachments(owner_id);

-- Счётчик живых привязок для быстрой проверки «можно ли удалять файлы».
-- Инкрементируется/декрементируется атомарно внутри транзакции attach/detach.
ALTER TABLE media
    ADD COLUMN IF NOT EXISTS usages_count INT NOT NULL DEFAULT 0;