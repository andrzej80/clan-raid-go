package domain

import (
	"context"

	"github.com/google/uuid"
)

// LeaderboardEntry — строка лидерборда рейда.
type LeaderboardEntry struct {
	PlayerID    int64
	TotalDamage int64
}

// DamageReader читает лидерборд из источника правды (Postgres).
type DamageReader interface {
	// TopDamage возвращает до limit игроков рейда по убыванию урона.
	TopDamage(ctx context.Context, raidID uuid.UUID, limit int) ([]LeaderboardEntry, error)
}
