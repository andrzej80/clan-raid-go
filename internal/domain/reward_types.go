package domain

import (
	"context"

	"github.com/google/uuid"
)

type RewardKind string

const (
	GeneralKind RewardKind = "participation"
	TopKind     RewardKind = "top3"
	KillerKind  RewardKind = "killing_blow"
)

type Reward struct {
	PlayerID int
	Kind     RewardKind
	Amount   int
}

// PlayerReward — награда игрока за конкретный рейд.
type PlayerReward struct {
	RaidID uuid.UUID
	Kind   RewardKind
	Amount int
}

// RewardsReader читает награды игрока.
type RewardsReader interface {
	// ListByPlayer возвращает до limit наград игрока, новые первыми.
	ListByPlayer(ctx context.Context, playerID int64, limit int) ([]PlayerReward, error)
}
