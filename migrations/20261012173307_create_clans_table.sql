-- +goose Up
CREATE TABLE clans (
  id         BIGSERIAL PRIMARY KEY,
  name       TEXT NOT NULL
);

-- +goose Down
DROP TABLE clans;
