CREATE EXTENSION IF NOT EXISTS pg_uuidv7;

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now(),
    status TEXT NOT NULL,
    error JSONB,
    exchange TEXT NOT NULL,
    routing_key TEXT NOT NULL
);

CREATE INDEX idx_outbox_events_created_at ON outbox_events (created_at);

CREATE PUBLICATION outbox_pub FOR
TABLE outbox_events
WITH (publish = 'insert');

CREATE ROLE relay WITH LOGIN REPLICATION PASSWORD '123456';

GRANT SELECT ON outbox_events TO relay;