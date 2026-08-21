DROP INDEX IF EXISTS uq_media_derivative_media_id_variant;

DROP INDEX IF EXISTS uq_processing_jobs_media_id_type;

DROP INDEX IF EXISTS idx_processing_jobs_lease;

ALTER TABLE processing_jobs
	DROP COLUMN IF EXISTS lease_until,
	DROP COLUMN IF EXISTS locked_by;
