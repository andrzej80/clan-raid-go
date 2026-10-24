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

var _ domain.OutboxRelayStore = (*OutboxRepo)(nil)

func (r *OutboxRepo) LockUnsent(ctx context.Context, limit int) ([]domain.OutboxEntry, error) {
	rows, err := conn(ctx, r.pool).Query(ctx, `
		SELECT id, topic, key, payload, headers
		FROM outbox
		WHERE sent_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("select unsent outbox: %w", err)
	}
	defer rows.Close()

	var entries []domain.OutboxEntry
	for rows.Next() {
		var e domain.OutboxEntry
		if err := rows.Scan(&e.ID, &e.Topic, &e.Key, &e.Payload, &e.Headers); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outbox: %w", err)
	}
	return entries, nil
}

func (r *OutboxRepo) MarkSent(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	return nil
}

func (r *OutboxRepo) CountUnsent(ctx context.Context) (int64, error) {
	var n int64
	if err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count unsent outbox: %w", err)
	}
	return n, nil
}
