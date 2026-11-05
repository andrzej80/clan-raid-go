package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// errDuplicateHit откатывает транзакцию ApplyHit: удар с таким hit_id уже записан.
var errDuplicateHit = errors.New("duplicate hit")

// SyncHitsRepo применяет один удар прямо в Postgres (режим ATTACK_MODE=sync): без очереди и без
// пакетной обработки. Это «голый» вариант эксперимента про горячую строку: меняются только raids и
// raid_hits, без raid_damage, outbox и Redis.
type SyncHitsRepo struct {
	pool *pgxpool.Pool
}

func NewSyncHitsRepo(pool *pgxpool.Pool) *SyncHitsRepo {
	return &SyncHitsRepo{pool: pool}
}

// ApplyHit за одну транзакцию списывает HP и записывает удар.
//
// Порядок важен. UPDATE идёт первым: он берёт блокировку строки рейда, и только потом INSERT получает
// seq, поэтому порядок seq совпадает с порядком применения (на нём держится инвариант 3).
// Блокировка держится до COMMIT, то есть на время INSERT и коммита: это и есть «горячая строка».
//
// Дубль hit_id откатывает транзакцию целиком, а значит, и списание HP; клиенту при этом возвращается
// успех, как и при повторе в режиме с очередью.
func (r *SyncHitsRepo) ApplyHit(ctx context.Context, hit domain.Hit) error {
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE raids SET
			  hp_left          = GREATEST(hp_left - $2::bigint, 0),
			  status           = CASE WHEN hp_left - $2::bigint <= 0 THEN 'defeated' ELSE status END,
			  finished_at      = CASE WHEN hp_left - $2::bigint <= 0 THEN now() ELSE finished_at END,
			  killer_player_id = CASE WHEN hp_left - $2::bigint <= 0 THEN $4::bigint ELSE killer_player_id END,
			  killer_hit_id    = CASE WHEN hp_left - $2::bigint <= 0 THEN $5::uuid ELSE killer_hit_id END
			WHERE id = $1 AND status = 'active' AND ends_at > $3::timestamptz`,
			hit.RaidID, int64(hit.Damage), hit.CreatedAt, hit.PlayerID, hit.ID)
		if err != nil {
			return fmt.Errorf("update raid hp: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrRaidNotActive
		}

		tag, err = tx.Exec(ctx, `
			INSERT INTO raid_hits (hit_id, raid_id, player_id, damage, is_crit, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (hit_id) DO NOTHING`,
			hit.ID, hit.RaidID, hit.PlayerID, int32(hit.Damage), hit.IsCrit, hit.CreatedAt) //nolint:gosec // урон — сотни-тысячи, колонка INT
		if err != nil {
			if isViolation(err, pgForeignKeyViolation, constraintHitsPlayerFK) {
				return domain.ErrPlayerNotFound
			}
			return fmt.Errorf("insert hit: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return errDuplicateHit
		}
		return nil
	})
	if errors.Is(err, errDuplicateHit) {
		return nil
	}
	return err
}
