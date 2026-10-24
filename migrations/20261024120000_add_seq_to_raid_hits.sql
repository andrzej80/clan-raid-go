-- +goose Up
-- seq фиксирует порядок применения ударов к рейду: консьюмер вставляет принятые удары в порядке
-- обработки под FOR UPDATE, поэтому по seq `raidctl verify` проверяет, что после killing blow ничего не принято.
ALTER TABLE raid_hits ADD COLUMN seq BIGINT GENERATED ALWAYS AS IDENTITY;
CREATE INDEX raid_hits_raid_seq_idx ON raid_hits(raid_id, seq);

-- +goose Down
DROP INDEX raid_hits_raid_seq_idx;
ALTER TABLE raid_hits DROP COLUMN seq;
