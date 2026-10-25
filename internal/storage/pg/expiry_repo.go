package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type ExpiryRepo struct {
	pool *pgxpool.Pool
}

func NewExpiryRepo(pool *pgxpool.Pool) *ExpiryRepo {
	return &ExpiryRepo{pool: pool}
}

var _ domain.ExpiryRepository = (*ExpiryRepo)(nil)

// ExpireDue время берёт из самого Postgres (now()), как и ends_at: часы приложения не участвуют.
// Если консьюмер ударов в этот момент держит строку рейда (FOR UPDATE), UPDATE ждёт его коммита и
// перепроверяет условие: рейд, убитый последним ударом, останется defeated.
func (r *ExpiryRepo) ExpireDue(ctx context.Context) ([]domain.ExpiredRaid, error) {
	rows, err := conn(ctx, r.pool).Query(ctx, `
		UPDATE raids
		SET status = 'expired', finished_at = now()
		WHERE status = 'active' AND ends_at < now()
		RETURNING id, hp_left, finished_at`)
	if err != nil {
		return nil, fmt.Errorf("expire raids: %w", err)
	}
	defer rows.Close()

	var out []domain.ExpiredRaid
	for rows.Next() {
		var e domain.ExpiredRaid
		if err := rows.Scan(&e.ID, &e.HPLeft, &e.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan expired raid: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("expire raids: %w", err)
	}
	return out, nil
}
