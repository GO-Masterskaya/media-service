DROP TRIGGER IF EXISTS trg_processing_jobs_set_updated_at ON processing_jobs;

DROP INDEX IF EXISTS idx_processing_jobs_claim;

DROP INDEX IF EXISTS idx_processing_jobs_media_id;

DROP TABLE IF EXISTS processing_jobs;

DROP INDEX IF EXISTS idx_media_derivative_media_id;

DROP TABLE IF EXISTS media_derivative;

DROP TRIGGER IF EXISTS trg_media_set_updated_at ON media;

DROP FUNCTION IF EXISTS set_updated_at();

DROP INDEX IF EXISTS idx_media_expires_at;

DROP INDEX IF EXISTS uq_media_owner_id_idempotency_key;

DROP INDEX IF EXISTS idx_media_owner_id;

DROP TABLE IF EXISTS media;

DROP TYPE IF EXISTS job_status;

DROP TYPE IF EXISTS media_status;

DROP TYPE IF EXISTS media_kind;
