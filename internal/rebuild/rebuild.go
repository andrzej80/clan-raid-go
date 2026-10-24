// Package rebuild восстанавливает кэш Redis (лидерборд и HP рейдов) из PostgreSQL.
package rebuild

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
)

const (
	defaultChunkSize = 1000
	// tmpKeyTTL подчищает временный ключ, если процесс упал до RENAME.
	tmpKeyTTL = 5 * time.Minute
)

// Querier — то, что нужно от *pgxpool.Pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Options struct {
	// ChunkSize — сколько игроков уходит в один ZADD.
	ChunkSize int
}

type Result struct {
	Raids   int
	Players int
}

type raidRow struct {
	id     uuid.UUID
	hpLeft int64
}

// Run перестраивает кэш указанных рейдов (nil — всех) по данным Postgres.
//
// Лидерборд собирается во временном ключе и подменяется одним RENAME: читатели никогда не видят
// пустой или недозаполненный лидерборд. Остальные ключи Redis (rate limit) не затрагиваются,
// FLUSHDB не используется. Команда рассчитана на остановленную нагрузку: консьюмер, пишущий
// параллельно, может на мгновение перезаписать значения более свежими, это безопасно.
func Run(ctx context.Context, db Querier, rc redis.Cmdable, raidIDs []uuid.UUID, opts Options) (Result, error) {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = defaultChunkSize
	}

	raids, err := listRaids(ctx, db, raidIDs)
	if err != nil {
		return Result{}, err
	}

	var res Result
	for _, r := range raids {
		players, err := rebuildRaid(ctx, db, rc, r, opts.ChunkSize)
		if err != nil {
			return res, fmt.Errorf("raid %s: %w", r.id, err)
		}
		res.Raids++
		res.Players += players
	}
	return res, nil
}

func listRaids(ctx context.Context, db Querier, raidIDs []uuid.UUID) ([]raidRow, error) {
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

	var out []raidRow
	for rows.Next() {
		var r raidRow
		if err := rows.Scan(&r.id, &r.hpLeft); err != nil {
			return nil, fmt.Errorf("scan raid: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func rebuildRaid(ctx context.Context, db Querier, rc redis.Cmdable, r raidRow, chunk int) (int, error) {
	members, err := loadDamage(ctx, db, r.id)
	if err != nil {
		return 0, err
	}

	lbKey := rdb.LeaderboardKey(r.id)
	if len(members) == 0 {
		// Никто не бил: лидерборда быть не должно, в том числе остатков от прошлых данных.
		pipe := rc.TxPipeline()
		pipe.Del(ctx, lbKey)
		pipe.Set(ctx, rdb.HPKey(r.id), r.hpLeft, 0)
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, fmt.Errorf("reset redis state: %w", err)
		}
		return 0, nil
	}

	tmpKey := rdb.LeaderboardTmpKey(r.id)
	fill := rc.Pipeline()
	for start := 0; start < len(members); start += chunk {
		fill.ZAdd(ctx, tmpKey, members[start:min(start+chunk, len(members))]...)
	}
	fill.Expire(ctx, tmpKey, tmpKeyTTL)
	if _, err := fill.Exec(ctx); err != nil {
		cleanup(rc, tmpKey) //nolint:contextcheck // исходный ctx мог быть отменён, чистка идёт в свежем
		return 0, fmt.Errorf("fill temporary leaderboard: %w", err)
	}

	swap := rc.TxPipeline()
	swap.Rename(ctx, tmpKey, lbKey)
	swap.Persist(ctx, lbKey) // RENAME переносит TTL временного ключа
	swap.Set(ctx, rdb.HPKey(r.id), r.hpLeft, 0)
	if _, err := swap.Exec(ctx); err != nil {
		cleanup(rc, tmpKey) //nolint:contextcheck // исходный ctx мог быть отменён, чистка идёт в свежем
		return 0, fmt.Errorf("swap leaderboard: %w", err)
	}
	return len(members), nil
}

func loadDamage(ctx context.Context, db Querier, raidID uuid.UUID) ([]redis.Z, error) {
	rows, err := db.Query(ctx, `SELECT player_id, total_damage FROM raid_damage WHERE raid_id = $1`, raidID)
	if err != nil {
		return nil, fmt.Errorf("read raid_damage: %w", err)
	}
	defer rows.Close()

	var out []redis.Z
	for rows.Next() {
		var player, total int64
		if err := rows.Scan(&player, &total); err != nil {
			return nil, fmt.Errorf("scan raid_damage: %w", err)
		}
		out = append(out, redis.Z{Score: float64(total), Member: strconv.FormatInt(player, 10)})
	}
	return out, rows.Err()
}

// cleanup удаляет временный ключ после неудачи; контекст свежий: исходный мог быть отменён.
func cleanup(rc redis.Cmdable, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = rc.Del(ctx, key).Err() //nolint:gosec // best effort, ключ и так протухнет по TTL
}
