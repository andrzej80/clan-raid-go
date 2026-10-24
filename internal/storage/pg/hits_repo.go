package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type HitsRepo struct {
	pool *pgxpool.Pool
}

func NewHitsRepo(pool *pgxpool.Pool) *HitsRepo {
	return &HitsRepo{pool: pool}
}

var _ domain.HitsRepository = (*HitsRepo)(nil)

func (r *HitsRepo) LockRaid(ctx context.Context, id uuid.UUID) (domain.RaidState, error) {
	var (
		state  domain.RaidState
		status string
	)
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT status, hp_left FROM raids WHERE id = $1 FOR UPDATE`, id,
	).Scan(&status, &state.HPLeft)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.RaidState{}, domain.ErrRaidNotFound
		}
		return domain.RaidState{}, fmt.Errorf("lock raid: %w", err)
	}
	state.Status = domain.RaidStatus(status)
	return state, nil
}

func (r *HitsRepo) ExistingHitIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]struct{}{}, nil
	}
	rows, err := conn(ctx, r.pool).Query(ctx,
		`SELECT hit_id FROM raid_hits WHERE hit_id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("select existing hits: %w", err)
	}
	defer rows.Close()

	found := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan hit id: %w", err)
		}
		found[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select existing hits: %w", err)
	}
	return found, nil
}

// InsertHits вставляет удары одним запросом. Без ON CONFLICT: повтор hit_id под блокировкой рейда
// означает ошибку логики, и транзакция должна упасть, а не молча удвоить урон в raid_damage.
func (r *HitsRepo) InsertHits(ctx context.Context, hits []domain.Hit) error {
	if len(hits) == 0 {
		return nil
	}
	var (
		ids     = make([]uuid.UUID, len(hits))
		raidIDs = make([]uuid.UUID, len(hits))
		players = make([]int64, len(hits))
		damages = make([]int32, len(hits))
		crits   = make([]bool, len(hits))
		created = make([]time.Time, len(hits))
	)
	for i, h := range hits {
		ids[i], raidIDs[i], players[i] = h.ID, h.RaidID, h.PlayerID
		damages[i], crits[i], created[i] = int32(h.Damage), h.IsCrit, h.CreatedAt //nolint:gosec // урон — сотни-тысячи, колонка INT
	}
	_, err := conn(ctx, r.pool).Exec(ctx, `
		INSERT INTO raid_hits (hit_id, raid_id, player_id, damage, is_crit, created_at)
		SELECT hit_id, raid_id, player_id, damage, is_crit, created_at
		FROM unnest($1::uuid[], $2::uuid[], $3::bigint[], $4::int[], $5::bool[], $6::timestamptz[])
			WITH ORDINALITY AS t(hit_id, raid_id, player_id, damage, is_crit, created_at, ord)
		ORDER BY ord`, // порядок вставки = порядок применения: от него зависит seq
		ids, raidIDs, players, damages, crits, created)
	if err != nil {
		if isViolation(err, pgForeignKeyViolation, constraintHitsPlayerFK) {
			return fmt.Errorf("insert hits: %w", domain.ErrPlayerNotFound)
		}
		return fmt.Errorf("insert hits: %w", err)
	}
	return nil
}

func (r *HitsRepo) AddDamage(ctx context.Context, raidID uuid.UUID, delta []domain.PlayerDamage) ([]domain.PlayerDamage, error) {
	if len(delta) == 0 {
		return nil, nil
	}
	var (
		players = make([]int64, len(delta))
		damages = make([]int64, len(delta))
		hits    = make([]int32, len(delta))
	)
	for i, d := range delta {
		players[i], damages[i], hits[i] = d.PlayerID, d.Damage, int32(d.Hits) //nolint:gosec // число ударов в пачке ограничено
	}
	// Игроки в delta уникальны (domain.AggregateDamage): иначе ON CONFLICT DO UPDATE
	// не может затронуть одну строку дважды в одном запросе.
	rows, err := conn(ctx, r.pool).Query(ctx, `
		INSERT INTO raid_damage (raid_id, player_id, total_damage, hits)
		SELECT $1, p, d, h FROM unnest($2::bigint[], $3::bigint[], $4::int[]) AS t(p, d, h)
		ON CONFLICT (raid_id, player_id) DO UPDATE
		SET total_damage = raid_damage.total_damage + EXCLUDED.total_damage,
		    hits         = raid_damage.hits + EXCLUDED.hits
		RETURNING player_id, total_damage, hits`,
		raidID, players, damages, hits)
	if err != nil {
		return nil, fmt.Errorf("upsert raid damage: %w", err)
	}
	defer rows.Close()

	totals := make([]domain.PlayerDamage, 0, len(delta))
	for rows.Next() {
		var t domain.PlayerDamage
		if err := rows.Scan(&t.PlayerID, &t.Damage, &t.Hits); err != nil {
			return nil, fmt.Errorf("scan raid damage: %w", err)
		}
		totals = append(totals, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("upsert raid damage: %w", err)
	}
	return totals, nil
}

func (r *HitsRepo) UpdateHP(ctx context.Context, raidID uuid.UUID, hpLeft int64, kill *domain.Kill) error {
	var err error
	if kill == nil {
		_, err = conn(ctx, r.pool).Exec(ctx,
			`UPDATE raids SET hp_left = $2 WHERE id = $1`, raidID, hpLeft)
	} else {
		_, err = conn(ctx, r.pool).Exec(ctx, `
			UPDATE raids
			SET hp_left = $2, status = 'defeated', finished_at = $3,
			    killer_player_id = $4, killer_hit_id = $5
			WHERE id = $1`,
			raidID, hpLeft, kill.FinishedAt, kill.PlayerID, kill.HitID)
	}
	if err != nil {
		return fmt.Errorf("update raid hp: %w", err)
	}
	return nil
}
