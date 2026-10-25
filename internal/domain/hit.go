package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Hit — принятый сервером удар, который уходит в очередь на обработку.
type Hit struct {
	ID        uuid.UUID // = idempotency_key клиента
	RaidID    uuid.UUID
	PlayerID  int64
	ClanID    int64
	Damage    int
	IsCrit    bool
	CreatedAt time.Time
}

// RaidState — часть строки raids, нужная консьюмеру ударов под блокировкой.
type RaidState struct {
	Status RaidStatus
	HPLeft int64
	EndsAt time.Time
}

// Kill — данные добивающего удара для перевода рейда в defeated.
type Kill struct {
	PlayerID   int64
	HitID      uuid.UUID
	FinishedAt time.Time
}

// OutboxMessage — запись transactional outbox; Payload — сериализованный protobuf.
type OutboxMessage struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// HitsRepository — операции над ударами; все вызываются внутри транзакции TxManager.
type HitsRepository interface {
	// LockRaid берёт строку рейда FOR UPDATE; ErrRaidNotFound, если рейда нет.
	LockRaid(ctx context.Context, id uuid.UUID) (RaidState, error)
	// ExistingHitIDs возвращает из ids те, что уже записаны в raid_hits.
	ExistingHitIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]struct{}, error)
	InsertHits(ctx context.Context, hits []Hit) error
	// AddDamage прибавляет урон к raid_damage и возвращает абсолютные суммы затронутых игроков.
	AddDamage(ctx context.Context, raidID uuid.UUID, delta []PlayerDamage) ([]PlayerDamage, error)
	// UpdateHP сохраняет hp_left; при kill != nil переводит рейд в defeated.
	UpdateHP(ctx context.Context, raidID uuid.UUID, hpLeft int64, kill *Kill) error
}

type OutboxRepository interface {
	Add(ctx context.Context, msg OutboxMessage) error
}

// RejectedHits — удары рейда, которые консьюмер не смог применить из-за неисправимой ошибки
// (например, игрока не существует). Повтор не поможет: такие удары уходят в DLQ.
type RejectedHits struct {
	RaidID uuid.UUID
	Hits   []Hit
	Reason string
}

// BatchResult — итог обработки пачки ударов.
type BatchResult struct {
	Rejected []RejectedHits
}

// OutboxEntry — запись outbox, прочитанная relay'ем.
type OutboxEntry struct {
	ID int64
	OutboxMessage
}

// OutboxRelayStore — операции relay над таблицей outbox; LockUnsent и MarkSent вызываются
// в одной транзакции TxManager.
type OutboxRelayStore interface {
	// LockUnsent блокирует до limit неотправленных записей (FOR UPDATE SKIP LOCKED) в порядке id.
	// Записи, заблокированные другим relay'ем, пропускаются.
	LockUnsent(ctx context.Context, limit int) ([]OutboxEntry, error)
	// MarkSent помечает записи отправленными.
	MarkSent(ctx context.Context, ids []int64) error
	// CountUnsent возвращает число неотправленных записей (для метрики).
	CountUnsent(ctx context.Context) (int64, error)
}
