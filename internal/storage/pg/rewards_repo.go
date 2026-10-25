package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type RewardsRepo struct {
	pool *pgxpool.Pool
}

func NewRewardsRepo(pool *pgxpool.Pool) *RewardsRepo {
	return &RewardsRepo{pool: pool}
}

var _ domain.RewardsRepository = (*RewardsRepo)(nil)

func (r *RewardsRepo) RaidOutcome(ctx context.Context, raidID uuid.UUID) (domain.RaidOutcome, error) {
	var (
		out    domain.RaidOutcome
		status string
	)
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT status, killer_player_id FROM raids WHERE id = $1`, raidID,
	).Scan(&status, &out.KillerPlayerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.RaidOutcome{}, domain.ErrRaidNotFound
		}
		return domain.RaidOutcome{}, fmt.Errorf("select raid outcome: %w", err)
	}
	out.Status = domain.RaidStatus(status)
	return out, nil
}

func (r *RewardsRepo) PlayersDamage(ctx context.Context, raidID uuid.UUID) ([]domain.PlayerDamage, error) {
	rows, err := conn(ctx, r.pool).Query(ctx,
		`SELECT player_id, total_damage, hits FROM raid_damage WHERE raid_id = $1 ORDER BY player_id`, raidID)
	if err != nil {
		return nil, fmt.Errorf("select players damage: %w", err)
	}
	defer rows.Close()

	var out []domain.PlayerDamage
	for rows.Next() {
		var d domain.PlayerDamage
		if err := rows.Scan(&d.PlayerID, &d.Damage, &d.Hits); err != nil {
			return nil, fmt.Errorf("scan players damage: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select players damage: %w", err)
	}
	return out, nil
}

// InsertRewards вставляет награды одним запросом. Уникальность (raid_id, player_id, kind) делает
// повторную выдачу безвредной: конфликтующие строки не вставляются и не возвращаются.
func (r *RewardsRepo) InsertRewards(
	ctx context.Context, raidID uuid.UUID, rewards []domain.Reward,
) ([]domain.Reward, error) {
	if len(rewards) == 0 {
		return nil, nil
	}
	players := make([]int64, len(rewards))
	kinds := make([]string, len(rewards))
	amounts := make([]int32, len(rewards))
	for i, rw := range rewards {
		players[i], kinds[i] = int64(rw.PlayerID), string(rw.Kind)
		amounts[i] = int32(rw.Amount) //nolint:gosec // награды малы
	}

	rows, err := conn(ctx, r.pool).Query(ctx, `
		INSERT INTO rewards (raid_id, player_id, kind, amount)
		SELECT $1, player_id, kind, amount
		FROM unnest($2::bigint[], $3::text[], $4::int[]) AS t(player_id, kind, amount)
		ON CONFLICT (raid_id, player_id, kind) DO NOTHING
		RETURNING player_id, kind, amount`, raidID, players, kinds, amounts)
	if err != nil {
		return nil, fmt.Errorf("insert rewards: %w", err)
	}
	defer rows.Close()

	var inserted []domain.Reward
	for rows.Next() {
		var (
			pid  int64
			kind string
			amt  int32
		)
		if err := rows.Scan(&pid, &kind, &amt); err != nil {
			return nil, fmt.Errorf("scan inserted reward: %w", err)
		}
		inserted = append(inserted, domain.Reward{PlayerID: int(pid), Kind: domain.RewardKind(kind), Amount: int(amt)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("insert rewards: %w", err)
	}
	return inserted, nil
}
