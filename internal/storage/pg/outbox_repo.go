package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type OutboxRepo struct {
	pool *pgxpool.Pool
}

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

var _ domain.OutboxRepository = (*OutboxRepo)(nil)

// Add пишет событие в outbox; вызывать в той же транзакции, что и изменение состояния.
func (r *OutboxRepo) Add(ctx context.Context, msg domain.OutboxMessage) error {
	headers, err := json.Marshal(msg.Headers)
	if err != nil {
		return fmt.Errorf("marshal outbox headers: %w", err)
	}
	if msg.Headers == nil {
		headers = []byte("{}")
	}
	_, err = conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3, $4)`,
		msg.Topic, msg.Key, msg.Payload, headers)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}
