package main

import (
	"context"
	"log/slog"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/scheduler"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func main() {
	app.Main("scheduler", run)
}

func run(ctx context.Context, cfg *config.Config) error {
	flushTelemetry, err := app.SetupTelemetry(ctx)
	if err != nil {
		return err
	}
	defer flushTelemetry()

	dbPool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	s, err := scheduler.New(pg.NewTxManager(dbPool), pg.NewExpiryRepo(dbPool), pg.NewOutboxRepo(dbPool),
		scheduler.Config{Interval: cfg.Scheduler.Interval, EventsTopic: cfg.Kafka.EventsTopic})
	if err != nil {
		return err
	}

	slog.Info("scheduler started", "interval", cfg.Scheduler.Interval, "topic", cfg.Kafka.EventsTopic)
	return s.Run(ctx)
}
