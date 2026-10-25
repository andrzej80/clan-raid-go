package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/rewards"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func main() {
	app.Main("rewards-worker", run)
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

	processor, err := rewards.NewProcessor(
		pg.NewTxManager(dbPool), pg.NewRewardsRepo(dbPool), pg.NewOutboxRepo(dbPool), cfg.Kafka.RewardsTopic)
	if err != nil {
		return err
	}

	consumer, err := kafka.NewRaidEventsConsumer(kafka.ConsumerConfig{
		Brokers:      cfg.Kafka.Brokers,
		Topic:        cfg.Kafka.EventsTopic,
		DLQTopic:     cfg.Kafka.EventsDLQTopic,
		Group:        cfg.Kafka.RewardsGroup,
		BatchSize:    cfg.Kafka.BatchSize,
		BatchTimeout: cfg.Kafka.BatchTimeout,
	}, processor)
	if err != nil {
		return err
	}
	// Close после Run: горутины партиций к этому моменту дообработали и закоммитили свои пачки,
	// а пул Postgres (defer выше) закрывается уже после выхода из группы.
	defer consumer.Close()

	slog.Info("rewards-worker started",
		"topic", cfg.Kafka.EventsTopic, "group", cfg.Kafka.RewardsGroup, "rewards_topic", cfg.Kafka.RewardsTopic)
	return consumer.Run(ctx)
}
