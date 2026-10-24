package pg

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pgUniqueViolation     = "23505"
	pgForeignKeyViolation = "23503"
)

// Имена ограничений из миграций.
const (
	constraintOneActiveRaidPerClan = "raids_one_active_per_clan"
	constraintRaidsClanFK          = "raids_clan_id_fkey"
	constraintHitsPlayerFK         = "raid_hits_player_id_fkey"
)

// dbtx — общий интерфейс для *pgxpool.Pool и pgx.Tx.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

// conn возвращает транзакцию из ctx, если она есть, иначе пул.
func conn(ctx context.Context, pool *pgxpool.Pool) dbtx {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return pool
}

// isViolation сообщает, что err — ошибка Postgres с кодом code на ограничении constraint.
func isViolation(err error, code, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code && pgErr.ConstraintName == constraint
}
