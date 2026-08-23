DROP TRIGGER IF EXISTS trg_processed_events_set_updated_at ON processed_events;
DROP TABLE IF EXISTS processed_events;
DROP TYPE IF EXISTS event_status;