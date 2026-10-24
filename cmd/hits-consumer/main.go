package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func main() {
	app.Main("hits-consumer", run)
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

	// Один пул на процесс. MaxConns — потолок одновременных транзакций (PG_MAX_CONNS): соединения
	// открываются по мере надобности, а при нехватке партиции ждут очереди, не теряя порядка.
	dbPool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	processor, err := raid.NewHitsProcessor(
		pg.NewTxManager(dbPool), pg.NewHitsRepo(dbPool), pg.NewOutboxRepo(dbPool), cfg.Kafka.EventsTopic)
	if err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:      cfg.Kafka.Brokers,
		Topic:        cfg.Kafka.HitsTopic,
		DLQTopic:     cfg.Kafka.HitsDLQTopic,
		Group:        cfg.Kafka.ConsumerGroup,
		BatchSize:    cfg.Kafka.BatchSize,
		BatchTimeout: cfg.Kafka.BatchTimeout,
	}, processor)
	if err != nil {
		return err
	}
	// Close после Run: к этому моменту горутины партиций дообработали и закоммитили свои пачки,
	// а пул Postgres (defer выше) закрывается уже после выхода из группы.
	defer consumer.Close()

	slog.Info("hits-consumer started",
		"topic", cfg.Kafka.HitsTopic, "group", cfg.Kafka.ConsumerGroup,
		"batch_size", cfg.Kafka.BatchSize, "batch_timeout", cfg.Kafka.BatchTimeout)
	return consumer.Run(ctx)
}
