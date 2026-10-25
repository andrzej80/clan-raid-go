package domain

import (
	"context"

	"github.com/google/uuid"
)

type RaidEventKind string

const (
	RaidEventDefeated RaidEventKind = "defeated"
	RaidEventExpired  RaidEventKind = "expired"
)

// RaidEvent — событие жизненного цикла рейда из топика raid.events.
type RaidEvent struct {
	RaidID uuid.UUID
	Kind   RaidEventKind
}

// RejectedEvent — событие, которое нельзя обработать (повтор не поможет); уходит в DLQ.
type RejectedEvent struct {
	Index  int // позиция события в пачке, переданной процессору
	Reason string
}

// RaidOutcome — итог рейда, нужный для расчёта наград.
type RaidOutcome struct {
	Status         RaidStatus
	KillerPlayerID *int64
}

// RewardsRepository — операции над наградами; вызываются внутри транзакции TxManager.
type RewardsRepository interface {
	// RaidOutcome читает статус и убийцу рейда; ErrRaidNotFound, если рейда нет.
	RaidOutcome(ctx context.Context, raidID uuid.UUID) (RaidOutcome, error)
	// PlayersDamage возвращает суммарный урон каждого игрока рейда из raid_damage.
	PlayersDamage(ctx context.Context, raidID uuid.UUID) ([]PlayerDamage, error)
	// InsertRewards вставляет награды, пропуская уже выданные (ON CONFLICT DO NOTHING), и возвращает
	// только реально вставленные строки.
	InsertRewards(ctx context.Context, raidID uuid.UUID, rewards []Reward) ([]Reward, error)
}
