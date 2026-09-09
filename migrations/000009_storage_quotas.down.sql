DROP TRIGGER IF EXISTS trg_media_storage_quota ON media;
DROP FUNCTION IF EXISTS update_storage_used_bytes();
DROP TABLE IF EXISTS storage_quotas;
