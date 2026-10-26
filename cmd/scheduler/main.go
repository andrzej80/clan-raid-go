package main

import (
	"context"
	"log/slog"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/scheduler"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
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

	schedulerCfg := scheduler.Config{Interval: cfg.Scheduler.Interval, EventsTopic: cfg.Kafka.EventsTopic}
	// Redis нужен только для уведомления подписчиков WatchRaid; без него scheduler работает как раньше.
	if cfg.Redis.URL != "" {
		redisClient, err := rdb.New(ctx, &cfg.Redis)
		if err != nil {
			return err
		}
		defer func() { _ = redisClient.Close() }() //nolint:gosec // закрытие при выходе, ошибку обработать нечем
		schedulerCfg.Publisher = rdb.NewBoard(redisClient.Redis())
	}

	s, err := scheduler.New(pg.NewTxManager(dbPool), pg.NewExpiryRepo(dbPool), pg.NewOutboxRepo(dbPool), schedulerCfg)
	if err != nil {
		return err
	}

	slog.Info("scheduler started", "interval", cfg.Scheduler.Interval, "topic", cfg.Kafka.EventsTopic)
	return s.Run(ctx)
}
