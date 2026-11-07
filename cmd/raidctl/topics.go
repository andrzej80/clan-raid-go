package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
)

// topicsRetryInterval — пауза между попытками apply, пока кластер не готов.
const topicsRetryInterval = 2 * time.Second

// errTopicsDrift — кластер расходится с описанием топиков в коде.
var errTopicsDrift = errors.New("topics differ from the spec")

func runTopics(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("topics: subcommand is required (diff, apply)\n\n%s", usage)
	}
	switch args[0] {
	case "diff":
		return runTopicsDiff(ctx, args[1:], out)
	case "apply":
		return runTopicsApply(ctx, args[1:], out)
	default:
		return fmt.Errorf("topics: unknown subcommand %q (diff, apply)\n\n%s", args[0], usage)
	}
}

// topicsAdmin открывает клиент Kafka по KAFKA_BROKERS и возвращает описание топиков из кода.
func topicsAdmin() (*kafka.TopicAdmin, []kafka.TopicSpec, func(), error) {
	cfg := config.LoadKafka()
	if len(cfg.Brokers) == 0 {
		return nil, nil, nil, errors.New("KAFKA_BROKERS is required")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("kafka client: %w", err)
	}
	specs := kafka.Topics(kafka.TopicsConfig{
		HitsTopic:         cfg.HitsTopic,
		HitsDLQTopic:      cfg.HitsDLQTopic,
		EventsTopic:       cfg.EventsTopic,
		EventsDLQTopic:    cfg.EventsDLQTopic,
		RewardsTopic:      cfg.RewardsTopic,
		ReplicationFactor: cfg.ReplicationFactor,
	})
	return kafka.NewTopicAdmin(cl), specs, cl.Close, nil
}

func runTopicsDiff(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("topics diff", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	adm, specs, closeClient, err := topicsAdmin()
	if err != nil {
		return err
	}
	defer closeClient()

	drifts, err := adm.Diff(ctx, specs)
	if err != nil {
		return err
	}
	if len(drifts) == 0 {
		_, err := fmt.Fprintln(out, "topics match the spec")
		return err
	}
	for _, d := range drifts {
		if _, err := fmt.Fprintf(out, "%s%s\n", d, fixableSuffix(d)); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %d difference(s)", errTopicsDrift, len(drifts))
}

func runTopicsApply(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("topics apply", flag.ContinueOnError)
	wait := fs.Duration("wait", time.Minute, "how long to retry while the cluster is unreachable or not ready")
	if err := fs.Parse(args); err != nil {
		return err
	}
	adm, specs, closeClient, err := topicsAdmin()
	if err != nil {
		return err
	}
	defer closeClient()

	applied, remaining, err := applyWithRetry(ctx, adm, specs, *wait)
	if err != nil {
		return err
	}
	for _, d := range applied {
		if _, err := fmt.Fprintf(out, "applied: %s\n", d); err != nil {
			return err
		}
	}
	if len(remaining) == 0 {
		_, err := fmt.Fprintln(out, "topics match the spec")
		return err
	}
	// Партиции и фактор репликации меняются только вручную, по runbook: молча продолжать
	// с топиком не той формы нельзя.
	for _, d := range remaining {
		if _, err := fmt.Fprintf(out, "not applied: %s (change manually, see runbook)\n", d); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %d difference(s) need manual action", errTopicsDrift, len(remaining))
}

// applyWithRetry повторяет Apply, пока кластер недоступен: в compose raidctl стартует сразу после
// healthcheck брокеров, а контроллер может ещё не видеть их все (создание с RF=3 падает).
func applyWithRetry(
	ctx context.Context, adm *kafka.TopicAdmin, specs []kafka.TopicSpec, wait time.Duration,
) (applied, remaining []kafka.Drift, err error) {
	deadline := time.Now().Add(wait)
	for {
		var done []kafka.Drift
		done, remaining, err = adm.Apply(ctx, specs)
		applied = append(applied, done...)
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return applied, remaining, err
		}
		slog.Warn("topics apply failed, will retry", "err", err)
		select {
		case <-time.After(topicsRetryInterval):
		case <-ctx.Done():
			return applied, remaining, ctx.Err()
		}
	}
}

func fixableSuffix(d kafka.Drift) string {
	if d.Fixable {
		return " (fixed by apply)"
	}
	return " (manual, see runbook)"
}
