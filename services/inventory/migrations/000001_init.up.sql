CREATE TABLE IF NOT EXISTS stock (
  sku TEXT PRIMARY KEY,
  available INT NOT NULL CHECK (available >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
  order_id BIGINT PRIMARY KEY,
  sku TEXT NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  reserved BOOLEAN NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL,
  key TEXT NOT NULL,
  payload BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
  ON outbox_events(id)
  WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed initial stock for learning/testing
INSERT INTO stock(sku, available) VALUES
  ('burger', 5),
  ('pizza', 10)
ON CONFLICT (sku) DO NOTHING;