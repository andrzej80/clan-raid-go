package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/health"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
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

	if cfg.Redis.URL == "" {
		return errors.New("REDIS_URL is required")
	}
	redisClient, err := rdb.New(ctx, &cfg.Redis)
	if err != nil {
		return err
	}
	// Закрывается после consumer.Close (defer выполняются в обратном порядке): пока партиции
	// дообрабатывают пачки, запись в Redis ещё нужна.
	defer func() { _ = redisClient.Close() }() //nolint:gosec // закрытие при выходе, ошибку обработать нечем

	board := rdb.NewBoard(redisClient.Redis())
	processor, err := raid.NewHitsProcessor(
		pg.NewTxManager(dbPool), pg.NewHitsRepo(dbPool), pg.NewOutboxRepo(dbPool), cfg.Kafka.EventsTopic,
		raid.WithBoard(board), raid.WithFinishPublisher(board))
	if err != nil {
		return err
	}

	heartbeat := health.NewHeartbeat()
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:      cfg.Kafka.Brokers,
		Topic:        cfg.Kafka.HitsTopic,
		DLQTopic:     cfg.Kafka.HitsDLQTopic,
		Group:        cfg.Kafka.ConsumerGroup,
		BatchSize:    cfg.Kafka.BatchSize,
		BatchTimeout: cfg.Kafka.BatchTimeout,
		Heartbeat:    heartbeat,
	}, processor)
	if err != nil {
		return err
	}
	// Close после Run: к этому моменту горутины партиций дообработали и закоммитили свои пачки,
	// а пул Postgres (defer выше) закрывается уже после выхода из группы.
	defer consumer.Close()

	// Redis в readiness нет: запись в него best-effort, без Redis удары всё равно применяются.
	stopProbes, err := app.StartProbes(ctx, cfg.Probe, heartbeat, app.PostgresCheck(dbPool), app.KafkaCheck(consumer))
	if err != nil {
		return err
	}
	defer stopProbes()

	slog.Info("hits-consumer started",
		"topic", cfg.Kafka.HitsTopic, "group", cfg.Kafka.ConsumerGroup,
		"batch_size", cfg.Kafka.BatchSize, "batch_timeout", cfg.Kafka.BatchTimeout)
	return consumer.Run(ctx)
}
