-- +goose Up
CREATE TABLE raid_damage (
  raid_id      UUID   NOT NULL REFERENCES raids(id),
  player_id    BIGINT NOT NULL REFERENCES players(id),
  total_damage BIGINT NOT NULL,
  hits         INT    NOT NULL,
  PRIMARY KEY (raid_id, player_id)
);

-- +goose Down
DROP TABLE raid_damage;
