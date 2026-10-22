-- +goose Up
CREATE TABLE players (
  id         BIGSERIAL PRIMARY KEY,
  clan_id    BIGINT NOT NULL REFERENCES clans(id),
  name       TEXT NOT NULL,
  power      INT  NOT NULL CHECK (power BETWEEN 100 AND 1000)
);

-- +goose Down
DROP TABLE players;