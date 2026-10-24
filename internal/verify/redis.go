package verify

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
)

const (
	nameLeaderboard = "redis leaderboard equals raid_damage"
	nameRedisHP     = "redis hp equals raids.hp_left"
	// maxExamples — сколько расхождений показано в одном нарушении.
	maxExamples = 3
)

type raidHP struct {
	id     uuid.UUID
	hpLeft int64
}

// RunRedis проверяет инвариант 6: лидерборд в Redis совпадает с raid_damage (и кэш HP — с raids.hp_left).
// Имеет смысл после остановки нагрузки: пока консьюмер пишет, Redis может отставать.
// raidIDs ограничивает проверку указанными рейдами; nil — все рейды.
func RunRedis(ctx context.Context, db Querier, rc redis.Cmdable, raidIDs []uuid.UUID) ([]Violation, error) {
	raids, err := listRaids(ctx, db, raidIDs)
	if err != nil {
		return nil, err
	}

	var violations []Violation
	for _, r := range raids {
		want, err := damageByPlayer(ctx, db, r.id)
		if err != nil {
			return nil, err
		}
		got, err := redisLeaderboard(ctx, rc, r.id)
		if err != nil {
			return nil, err
		}
		if diffs := diffLeaderboard(want, got); len(diffs) > 0 {
			violations = append(violations, Violation{
				Invariant: 6, Name: nameLeaderboard, Subject: r.id.String(),
				Detail: fmt.Sprintf("%d differences, e.g. %s",
					len(diffs), strings.Join(diffs[:min(len(diffs), maxExamples)], "; ")),
			})
		}

		hp, err := rc.Get(ctx, rdb.HPKey(r.id)).Int64()
		switch {
		case errors.Is(err, redis.Nil):
			violations = append(violations, Violation{
				Name: nameRedisHP, Subject: r.id.String(), Detail: fmt.Sprintf("key is missing, hp_left=%d", r.hpLeft),
			})
		case err != nil:
			return nil, fmt.Errorf("read hp of raid %s: %w", r.id, err)
		case hp != r.hpLeft:
			violations = append(violations, Violation{
				Name: nameRedisHP, Subject: r.id.String(), Detail: fmt.Sprintf("redis=%d, postgres=%d", hp, r.hpLeft),
			})
		}
	}
	return violations, nil
}

func listRaids(ctx context.Context, db Querier, raidIDs []uuid.UUID) ([]raidHP, error) {
	var filter any
	if raidIDs != nil {
		filter = raidIDs
	}
	rows, err := db.Query(ctx,
		`SELECT id, hp_left FROM raids WHERE ($1::uuid[] IS NULL OR id = ANY($1)) ORDER BY id`, filter)
	if err != nil {
		return nil, fmt.Errorf("list raids: %w", err)
	}
	defer rows.Close()

	var out []raidHP
	for rows.Next() {
		var r raidHP
		if err := rows.Scan(&r.id, &r.hpLeft); err != nil {
			return nil, fmt.Errorf("scan raid: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func damageByPlayer(ctx context.Context, db Querier, raidID uuid.UUID) (map[int64]int64, error) {
	rows, err := db.Query(ctx, `SELECT player_id, total_damage FROM raid_damage WHERE raid_id = $1`, raidID)
	if err != nil {
		return nil, fmt.Errorf("read raid_damage of raid %s: %w", raidID, err)
	}
	defer rows.Close()

	out := make(map[int64]int64)
	for rows.Next() {
		var player, total int64
		if err := rows.Scan(&player, &total); err != nil {
			return nil, fmt.Errorf("scan raid_damage: %w", err)
		}
		out[player] = total
	}
	return out, rows.Err()
}

func redisLeaderboard(ctx context.Context, rc redis.Cmdable, raidID uuid.UUID) (map[int64]int64, error) {
	zs, err := rc.ZRangeWithScores(ctx, rdb.LeaderboardKey(raidID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read leaderboard of raid %s: %w", raidID, err)
	}
	out := make(map[int64]int64, len(zs))
	for _, z := range zs {
		member, _ := z.Member.(string)
		player, err := strconv.ParseInt(member, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("leaderboard of raid %s: bad member %q: %w", raidID, member, err)
		}
		out[player] = int64(z.Score)
	}
	return out, nil
}

// diffLeaderboard описывает расхождения между Postgres (want) и Redis (got), по игрокам в порядке id.
func diffLeaderboard(want, got map[int64]int64) []string {
	players := make(map[int64]struct{}, len(want)+len(got))
	for p := range want {
		players[p] = struct{}{}
	}
	for p := range got {
		players[p] = struct{}{}
	}
	ids := make([]int64, 0, len(players))
	for p := range players {
		ids = append(ids, p)
	}
	slices.Sort(ids)

	var diffs []string
	for _, p := range ids {
		w, inPG := want[p]
		g, inRedis := got[p]
		switch {
		case inPG && !inRedis:
			diffs = append(diffs, fmt.Sprintf("player %d: missing in redis, postgres=%d", p, w))
		case !inPG && inRedis:
			diffs = append(diffs, fmt.Sprintf("player %d: redis=%d, missing in postgres", p, g))
		case w != g:
			diffs = append(diffs, fmt.Sprintf("player %d: redis=%d, postgres=%d", p, g, w))
		}
	}
	return diffs
}
