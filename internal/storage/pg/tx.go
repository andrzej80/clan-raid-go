package pg

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

const tracerName = "github.com/andrzej80/clan-raid-go/internal/storage/pg"

type TxManager struct {
	pool   *pgxpool.Pool
	tracer trace.Tracer
}

type TxOption func(*TxManager)

// WithTracerProvider подменяет провайдер трейсов (по умолчанию глобальный).
func WithTracerProvider(tp trace.TracerProvider) TxOption {
	return func(m *TxManager) { m.tracer = tp.Tracer(tracerName) }
}

func NewTxManager(pool *pgxpool.Pool, opts ...TxOption) *TxManager {
	m := &TxManager{pool: pool, tracer: otel.Tracer(tracerName)}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// WithinTx выполняет fn в транзакции: commit при nil, иначе rollback.
// Вложенный вызов переиспользует уже открытую транзакцию.
func (m *TxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx)
	}

	// Span транзакции — родитель для span'ов её запросов (их создаёт трейсер пула): в трейсе видно,
	// сколько длилась транзакция целиком, включая ожидание соединения и блокировку FOR UPDATE.
	ctx, span := m.tracer.Start(ctx, "pg.tx")
	defer func() {
		telemetry.RecordError(span, err)
		span.End()
	}()

	return pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		return fn(context.WithValue(ctx, txKey{}, tx))
	})
}
