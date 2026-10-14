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
