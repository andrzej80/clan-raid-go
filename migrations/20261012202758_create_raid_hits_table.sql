-- +goose Up
CREATE TABLE raid_hits (
  hit_id     UUID PRIMARY KEY,              -- = idempotency_key, защищает от дублей
  raid_id    UUID   NOT NULL REFERENCES raids(id),
  player_id  BIGINT NOT NULL REFERENCES players(id),
  damage     INT    NOT NULL,
  is_crit    BOOL   NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX raid_hits_raid_id_idx ON raid_hits(raid_id);

-- +goose Down
DROP INDEX raid_hits_raid_id_idx;
DROP TABLE raid_hits;
