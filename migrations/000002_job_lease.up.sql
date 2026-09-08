ALTER TABLE processing_jobs
	ADD COLUMN locked_by text,
	ADD COLUMN lease_until timestamp with time zone;

CREATE INDEX idx_processing_jobs_lease
ON processing_jobs (lease_until)
WHERE status = 'running';

-- One logical job per (media, type): enqueue is idempotent.
CREATE UNIQUE INDEX uq_processing_jobs_media_id_type
ON processing_jobs (media_id, type);

-- One derivative per (media, variant).
CREATE UNIQUE INDEX uq_media_derivative_media_id_variant
ON media_derivative (media_id, variant);
