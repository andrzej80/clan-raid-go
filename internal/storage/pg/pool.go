package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
)

type DbConfig struct {
	Dsn             string
	MaxConns        int32
	MinConns        int32
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
	// TracerProvider необязателен: по умолчанию используется глобальный (см. telemetry.Setup).
	TracerProvider trace.TracerProvider
}

func NewPool(ctx context.Context, cfg *DbConfig) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.Dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot parse DATABASE_URL %w", err)
	}

	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	// Span на каждый запрос, батч и CopyFrom. В имя span'а идёт только тип операции (SELECT, INSERT), а не
	// весь SQL: иначе имена разрастаются. Значения параметров в span'ы не попадают.
	var traceOpts []otelpgx.Option
	if cfg.TracerProvider != nil {
		traceOpts = append(traceOpts, otelpgx.WithTracerProvider(cfg.TracerProvider))
	}
	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer(traceOpts...)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot create pool %w", err)
	}
	// Метрики пула: занятые и свободные соединения, число и время ожидания свободного соединения
	// (pgxpool.empty_acquire*) — по ним видно голодание пула. Читаются при экспорте метрик.
	if err := otelpgx.RecordStats(pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("cannot record pool stats %w", err)
	}
	return pool, nil
}
