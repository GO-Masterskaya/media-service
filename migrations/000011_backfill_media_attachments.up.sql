-- Backfill media_attachments для строк, созданных после 000005 без
-- INSERT attachment (InsertWithJobs до фикса в #89): иначе DeleteMedia
-- и Kafka detach на них — NotFound / no-op.
-- Логика как в 000005: owner_id → attachment, пересчёт usages_count.

INSERT INTO media_attachments (media_id, owner_id)
SELECT id, owner_id FROM media
WHERE owner_id IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE media m
SET usages_count = COALESCE((SELECT COUNT(*) FROM media_attachments a WHERE a.media_id = m.id), 0)
WHERE m.usages_count = 0;
