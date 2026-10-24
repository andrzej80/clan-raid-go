-- +goose Up
-- scheduler каждые N секунд ищет просроченные активные рейды: индекс покрывает только active-строки,
-- поэтому остаётся маленьким, сколько бы рейдов ни завершилось.
CREATE INDEX raids_active_ends_at_idx ON raids(ends_at) WHERE status = 'active';
-- ListRewards читает награды одного игрока, новые первыми.
CREATE INDEX rewards_player_idx ON rewards(player_id, id DESC);

-- +goose Down
DROP INDEX rewards_player_idx;
DROP INDEX raids_active_ends_at_idx;
