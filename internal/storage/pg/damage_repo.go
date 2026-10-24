package pg

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type DamageRepo struct {
	pool *pgxpool.Pool
}

func NewDamageRepo(pool *pgxpool.Pool) *DamageRepo {
	return &DamageRepo{pool: pool}
}

var _ domain.DamageReader = (*DamageRepo)(nil)

// TopDamage читает строки одного рейда по префиксу первичного ключа (raid_id, player_id) и
// берёт top-N сортировкой: игроков в рейде столько, сколько в клане, так что запрос дешёвый.
// Отдельный индекс по total_damage не нужен и вредил бы: он обновлялся бы на каждый удар.
func (r *DamageRepo) TopDamage(ctx context.Context, raidID uuid.UUID, limit int) ([]domain.LeaderboardEntry, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT player_id, total_damage
		FROM raid_damage
		WHERE raid_id = $1
		ORDER BY total_damage DESC, player_id
		LIMIT $2`, raidID, limit)
	if err != nil {
		return nil, fmt.Errorf("select top damage: %w", err)
	}
	defer rows.Close()

	var out []domain.LeaderboardEntry
	for rows.Next() {
		var e domain.LeaderboardEntry
		if err := rows.Scan(&e.PlayerID, &e.TotalDamage); err != nil {
			return nil, fmt.Errorf("scan top damage: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select top damage: %w", err)
	}
	return out, nil
}
