package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type RaidRepo struct {
	pool *pgxpool.Pool
}

func NewRaidRepo(pool *pgxpool.Pool) *RaidRepo {
	return &RaidRepo{pool: pool}
}

var _ domain.RaidRepository = (*RaidRepo)(nil)

const raidColumns = `id, clan_id, boss_name, hp_max, hp_left, status,
	started_at, ends_at, finished_at, killer_player_id, killer_hit_id`

func (r *RaidRepo) CreateRaid(ctx context.Context, raid *domain.Raid) error {
	_, err := conn(ctx, r.pool).Exec(ctx, `
		INSERT INTO raids (`+raidColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		raid.ID, raid.ClanID, raid.BossName, raid.HPMax, raid.HPLeft, string(raid.Status),
		raid.StartedAt, raid.EndsAt, raid.FinishedAt, raid.KillerPlayerID, raid.KillerHitID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrActiveRaidExists
		}
		return fmt.Errorf("insert raid: %w", err)
	}
	return nil
}

func (r *RaidRepo) GetRaid(ctx context.Context, id uuid.UUID) (*domain.Raid, error) {
	var (
		raid   domain.Raid
		status string
	)
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+raidColumns+` FROM raids WHERE id = $1`, id,
	).Scan(
		&raid.ID, &raid.ClanID, &raid.BossName, &raid.HPMax, &raid.HPLeft, &status,
		&raid.StartedAt, &raid.EndsAt, &raid.FinishedAt, &raid.KillerPlayerID, &raid.KillerHitID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrRaidNotFound
		}
		return nil, fmt.Errorf("get raid: %w", err)
	}
	raid.Status = domain.RaidStatus(status)
	return &raid, nil
}
