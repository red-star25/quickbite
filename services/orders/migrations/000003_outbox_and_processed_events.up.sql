CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ NULL,
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events(id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
)