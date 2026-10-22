-- +goose Up
CREATE TABLE outbox (
  id         BIGSERIAL PRIMARY KEY,
  topic      TEXT  NOT NULL,
  key        TEXT  NOT NULL,
  payload    BYTEA NOT NULL,                 
  headers    JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at    TIMESTAMPTZ
);
CREATE INDEX outbox_unsent ON outbox(id) WHERE sent_at IS NULL;

-- +goose Down
DROP INDEX outbox_unsent;
DROP TABLE outbox;
