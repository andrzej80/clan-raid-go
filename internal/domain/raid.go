package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type RaidRepository interface {
	CreateRaid(ctx context.Context, r *Raid) error
	GetRaid(ctx context.Context, id uuid.UUID) (*Raid, error)
}

type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type RaidStatus string

const (
	RaidActive   RaidStatus = "active"
	RaidDefeated RaidStatus = "defeated"
	RaidExpired  RaidStatus = "expired"
)

type Raid struct {
	ID             uuid.UUID
	ClanID         int64
	BossName       string
	HPMax          int64
	HPLeft         int64
	Status         RaidStatus
	StartedAt      time.Time
	EndsAt         time.Time
	FinishedAt     *time.Time
	KillerPlayerID *int64
	KillerHitID    *uuid.UUID
}

// ExpiredRaid — рейд, который scheduler только что перевёл в expired.
type ExpiredRaid struct {
	ID         uuid.UUID
	HPLeft     int64
	FinishedAt time.Time
}

type ExpiryRepository interface {
	// ExpireDue переводит просроченные активные рейды в expired и возвращает их. Условие
	// status = 'active' делает вызов безопасным из нескольких экземпляров: строку забирает один.
	ExpireDue(ctx context.Context) ([]ExpiredRaid, error)
}
