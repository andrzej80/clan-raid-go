-- +goose Up
CREATE TABLE rewards (
  id         BIGSERIAL PRIMARY KEY,
  raid_id    UUID   NOT NULL REFERENCES raids(id),
  player_id  BIGINT NOT NULL REFERENCES players(id),
  kind       TEXT   NOT NULL CHECK (kind IN ('participation','top3','killing_blow')),
  amount     INT    NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (raid_id, player_id, kind)          -- идемпотентность начисления
);

-- +goose Down
DROP TABLE rewards;
