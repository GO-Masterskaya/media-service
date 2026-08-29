ALTER TABLE media
	DROP COLUMN IF EXISTS params_fingerprint,
	DROP COLUMN IF EXISTS body_fingerprint;
