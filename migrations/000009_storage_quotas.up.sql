CREATE TABLE storage_quotas (
	owner_id uuid NOT NULL,
	storage_used_bytes bigint NOT NULL DEFAULT 0,
	storage_quota_bytes bigint NOT NULL DEFAULT 0,
	updated_at timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT storage_quotas_pkey PRIMARY KEY (owner_id),
	CONSTRAINT chk_storage_used_bytes_non_negative CHECK (storage_used_bytes >= 0)
);

INSERT INTO storage_quotas (owner_id, storage_used_bytes)
SELECT owner_id, COALESCE(SUM(size_bytes), 0)
FROM media
WHERE status <> 'failed'
GROUP BY owner_id
ON CONFLICT (owner_id) DO UPDATE
SET storage_used_bytes = EXCLUDED.storage_used_bytes;

CREATE OR REPLACE FUNCTION update_storage_used_bytes()
RETURNS trigger AS $$
BEGIN
	IF TG_OP = 'INSERT' THEN
		IF NEW.status <> 'failed' THEN
			INSERT INTO storage_quotas (owner_id, storage_used_bytes, updated_at)
			VALUES (NEW.owner_id, NEW.size_bytes, now())
			ON CONFLICT (owner_id) DO UPDATE
			SET storage_used_bytes = storage_quotas.storage_used_bytes + EXCLUDED.storage_used_bytes,
				updated_at = now();
		END IF;
		RETURN NEW;
	ELSIF TG_OP = 'DELETE' THEN
		IF OLD.status <> 'failed' THEN
			UPDATE storage_quotas
			SET storage_used_bytes = GREATEST(0, storage_used_bytes - OLD.size_bytes),
				updated_at = now()
			WHERE owner_id = OLD.owner_id;
		END IF;
		RETURN OLD;
	ELSIF TG_OP = 'UPDATE' THEN
		IF OLD.owner_id = NEW.owner_id THEN
			IF OLD.status <> 'failed' AND NEW.status = 'failed' THEN
				UPDATE storage_quotas
				SET storage_used_bytes = GREATEST(0, storage_used_bytes - OLD.size_bytes),
					updated_at = now()
				WHERE owner_id = OLD.owner_id;
			ELSIF OLD.status = 'failed' AND NEW.status <> 'failed' THEN
				INSERT INTO storage_quotas (owner_id, storage_used_bytes, updated_at)
				VALUES (NEW.owner_id, NEW.size_bytes, now())
				ON CONFLICT (owner_id) DO UPDATE
				SET storage_used_bytes = storage_quotas.storage_used_bytes + EXCLUDED.storage_used_bytes,
					updated_at = now();
			ELSIF OLD.size_bytes <> NEW.size_bytes AND NEW.status <> 'failed' THEN
				UPDATE storage_quotas
				SET storage_used_bytes = GREATEST(0, storage_used_bytes + (NEW.size_bytes - OLD.size_bytes)),
					updated_at = now()
				WHERE owner_id = NEW.owner_id;
			END IF;
		END IF;
		RETURN NEW;
	END IF;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_media_storage_quota
AFTER INSERT OR UPDATE OR DELETE ON media
FOR EACH ROW
EXECUTE FUNCTION update_storage_used_bytes();
