ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS broker TEXT NOT NULL DEFAULT 'rabbitmq',
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now(),
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_outbox_events_status_created
    ON outbox_events (status, created_at);

CREATE TABLE IF NOT EXISTS outbox_logs (
    id UUID PRIMARY KEY,
    outbox_id UUID,
    broker TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_logs_outbox_id ON outbox_logs (outbox_id);
CREATE INDEX IF NOT EXISTS idx_outbox_logs_created_at ON outbox_logs (created_at);
