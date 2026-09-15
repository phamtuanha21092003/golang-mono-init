DROP INDEX IF EXISTS idx_outbox_logs_created_at;
DROP INDEX IF EXISTS idx_outbox_logs_outbox_id;
DROP TABLE IF EXISTS outbox_logs;

DROP INDEX IF EXISTS idx_outbox_events_status_created;

ALTER TABLE outbox_events
    DROP COLUMN IF EXISTS published_at,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS broker;
