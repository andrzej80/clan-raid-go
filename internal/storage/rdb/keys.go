package rdb

import (
	"strconv"

	"github.com/google/uuid"
)

// Ключи собраны в одном месте, чтобы писатель и читатель не разошлись.

// RateLimitKey — HASH token bucket игрока.
func RateLimitKey(playerID int64) string { return "rl:" + strconv.FormatInt(playerID, 10) }

// LeaderboardKey — ZSET: member = player_id, score = абсолютный total_damage.
func LeaderboardKey(raidID uuid.UUID) string { return "lb:" + raidID.String() }

// HPKey — оставшееся HP рейда.
func HPKey(raidID uuid.UUID) string { return "raid:" + raidID.String() + ":hp" }

// UpdatesChannel — канал pub/sub обновлений рейда (читает WatchRaid).
func UpdatesChannel(raidID uuid.UUID) string { return "raid:" + raidID.String() + ":updates" }

// LeaderboardTmpKey — временный ZSET, из которого rebuild-cache атомарно (RENAME) подменяет лидерборд.
func LeaderboardTmpKey(raidID uuid.UUID) string {
	return LeaderboardKey(raidID) + ":rebuild:" + uuid.NewString()
}
