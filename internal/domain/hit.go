package domain

import (
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
