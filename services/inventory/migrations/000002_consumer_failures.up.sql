CREATE TABLE IF NOT EXISTS consumer_failures (
  topic TEXT NOT NULL,
  "partition" INT NOT NULL,
  "offset" BIGINT NOT NULL,
  group_id TEXT NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(topic, "partition", "offset", group_id)
);