-- Body and significant-params fingerprints for strict upload idempotency
-- (owner_id, idempotency_key) via PersistUpload / MediaRepo.InsertWithJobs.
ALTER TABLE media
	ADD COLUMN body_fingerprint text NOT NULL DEFAULT '',
	ADD COLUMN params_fingerprint text NOT NULL DEFAULT '';
