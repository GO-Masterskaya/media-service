CREATE TYPE media_kind AS ENUM
    ('image', 'video', 'audio');

CREATE TYPE media_status AS ENUM
    ('stored', 'processing', 'ready', 'failed', 'deleting');

CREATE TYPE job_status AS ENUM
    ('queued', 'running', 'done', 'failed');

CREATE TABLE media (
	id uuid NOT NULL,
	owner_id uuid NOT NULL,
	kind media_kind NOT NULL,
	orig_filename text NOT NULL,
	mime text NOT NULL,
	size_bytes bigint NOT NULL,
	status media_status NOT NULL,
	storage_key text NOT NULL,
	metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
	idempotency_key text NOT NULL,
	expires_at timestamp with time zone,
	error text,
	created_at timestamp with time zone DEFAULT now() NOT NULL,
	updated_at timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT media_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_media_owner_id ON media (owner_id);

CREATE UNIQUE INDEX uq_media_owner_id_idempotency_key ON media (owner_id, idempotency_key);

CREATE INDEX idx_media_expires_at
ON media (expires_at)
WHERE expires_at IS NOT NULL;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_media_set_updated_at
BEFORE UPDATE ON media
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE media_derivative (
	id uuid NOT NULL,
	media_id uuid NOT NULL,
	variant text NOT NULL,
	mime text NOT NULL,
	size_bytes bigint NOT NULL,
	storage_key text NOT NULL,
	metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
	created_at timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT media_derivative_pkey PRIMARY KEY (id),
	CONSTRAINT media_derivative_media_id_fkey FOREIGN KEY (media_id)
		REFERENCES media (id) ON DELETE CASCADE
);

CREATE INDEX idx_media_derivative_media_id ON media_derivative (media_id);

CREATE TABLE processing_jobs (
	id uuid NOT NULL,
	media_id uuid NOT NULL,
	type text NOT NULL,
	status job_status DEFAULT 'queued'::job_status NOT NULL,
	attempts integer DEFAULT 0 NOT NULL,
	last_error text,
	run_after timestamp with time zone DEFAULT now() NOT NULL,
	locked_at timestamp with time zone,
	created_at timestamp with time zone DEFAULT now() NOT NULL,
	updated_at timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT processing_jobs_pkey PRIMARY KEY (id),
	CONSTRAINT processing_jobs_media_id_fkey FOREIGN KEY (media_id)
		REFERENCES media (id) ON DELETE CASCADE
);

CREATE INDEX idx_processing_jobs_media_id ON processing_jobs (media_id);

-- Supports the worker claim query:
--   SELECT ... FROM processing_jobs
--   WHERE status = 'queued' AND run_after <= now()
--   ORDER BY run_after
--   FOR UPDATE SKIP LOCKED
CREATE INDEX idx_processing_jobs_claim
ON processing_jobs (status, run_after);

CREATE TRIGGER trg_processing_jobs_set_updated_at
BEFORE UPDATE ON processing_jobs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

