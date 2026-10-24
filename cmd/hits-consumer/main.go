package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

// telemetryShutdownTimeout — сколько ждём финального сброса метрик и трейсов в коллектор.
const telemetryShutdownTimeout = 5 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
	if err := run(cfg); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
	slog.Info("hits-consumer stopped")
}

func run(cfg *config.Config) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Setup(ctx)
	if err != nil {
		return err
	}
	defer func() {
		// Контекст создаётся в момент остановки: родительский ctx уже отменён сигналом.
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryShutdownTimeout)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			slog.Warn("telemetry shutdown", "err", err)
		}
	}()

	// Один пул на процесс. MaxConns должен покрывать число одновременно обрабатываемых партиций
	// (PG_MAX_CONNS): каждая держит по одной транзакции.
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
