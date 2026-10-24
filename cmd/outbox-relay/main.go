package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/outbox"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

// publisherCloseTimeout — сколько ждём сброса буфера продюсера при остановке.
const publisherCloseTimeout = 5 * time.Second

func main() {
	app.Main("outbox-relay", run)
}

func run(ctx context.Context, cfg *config.Config) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required")
	}

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

	publisher, err := kafka.NewPublisher(cfg.Kafka.Brokers)
	if err != nil {
		return err
	}
	defer func() { //nolint:contextcheck // сброс при остановке: родительский ctx уже отменён
		closeCtx, cancel := context.WithTimeout(context.Background(), publisherCloseTimeout)
		defer cancel()
		if err := publisher.Close(closeCtx); err != nil {
			slog.Warn("publisher close", "err", err)
		}
	}()

	relay, err := outbox.NewRelay(pg.NewTxManager(dbPool), pg.NewOutboxRepo(dbPool), publisher, outbox.Config{
		BatchSize:    cfg.Outbox.BatchSize,
		PollInterval: cfg.Outbox.PollInterval,
	})
	if err != nil {
		return err
	}

	slog.Info("outbox-relay started", "batch_size", cfg.Outbox.BatchSize, "poll_interval", cfg.Outbox.PollInterval)
	return relay.Run(ctx)
}
