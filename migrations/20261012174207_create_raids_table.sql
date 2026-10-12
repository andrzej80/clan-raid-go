-- +goose Up
CREATE TABLE raids (
  id               UUID PRIMARY KEY,
  clan_id          BIGINT NOT NULL REFERENCES clans(id),
  boss_name        TEXT   NOT NULL,
  hp_max           BIGINT NOT NULL CHECK (hp_max > 0),
  hp_left          BIGINT NOT NULL,
  status           TEXT   NOT NULL CHECK (status IN ('active','defeated','expired')),
  started_at       TIMESTAMPTZ NOT NULL,
  ends_at          TIMESTAMPTZ NOT NULL,
  finished_at      TIMESTAMPTZ,
  killer_player_id BIGINT REFERENCES players(id),
  killer_hit_id    UUID
);
-- правило «не больше одного активного рейда на клан»
CREATE UNIQUE INDEX raids_one_active_per_clan ON raids(clan_id) WHERE status = 'active';

-- +goose Down
DROP INDEX raids_one_active_per_clan;
DROP TABLE raids;