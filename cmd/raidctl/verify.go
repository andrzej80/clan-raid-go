package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
	"github.com/andrzej80/clan-raid-go/internal/verify"
)

var errInvariantsViolated = errors.New("invariants violated")

func runVerify(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	raidID := fs.String("raid", "", "check only this raid (default: all raids)")
	requireRewards := fs.Bool("require-rewards", false,
		"also fail on defeated raids without rewards (run after rewards-worker drained raid.events)")

	// Redis проверяется, если задан REDIS_URL; --redis=false отключает проверку явно.
	// Загрузка конфига до разбора флагов нужна ради значения по умолчанию.
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	checkRedis := fs.Bool("redis", cfg.Redis.URL != "", "also check invariant 6: Redis matches PostgreSQL (needs REDIS_URL; run after the load stopped)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *checkRedis && cfg.Redis.URL == "" {
		return errors.New("--redis requires REDIS_URL")
	}

	var raidIDs []uuid.UUID
	if *raidID != "" {
		id, err := uuid.Parse(*raidID)
		if err != nil {
			return fmt.Errorf("invalid --raid: %w", err)
		}
		raidIDs = []uuid.UUID{id}
	}

	pool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer pool.Close()

	violations, err := verify.Run(ctx, pool, raidIDs)
	if err != nil {
		return err
	}
	rewardViolations, err := verify.RunRewards(ctx, pool, raidIDs, *requireRewards)
	if err != nil {
		return err
	}
	violations = append(violations, rewardViolations...)
	if *checkRedis {
		redisClient, err := rdb.New(ctx, &cfg.Redis)
		if err != nil {
			return err
		}
		defer func() { _ = redisClient.Close() }() //nolint:gosec // закрытие при выходе, ошибку обработать нечем

		redisViolations, err := verify.RunRedis(ctx, pool, redisClient.Redis(), raidIDs)
		if err != nil {
			return err
		}
		violations = append(violations, redisViolations...)
	}
	if len(violations) == 0 {
		_, err = fmt.Fprintln(out, "OK: all invariants hold")
		return err
	}
	for _, v := range violations {
		if _, err := fmt.Fprintln(out, v); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %d found", errInvariantsViolated, len(violations))
}

func runMigrate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	dir := fs.String("dir", "migrations", "directory with goose migrations")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	pool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(pool)
	defer func() {
		if err := db.Close(); err != nil {
			slog.Warn("close db", "err", err)
		}
	}()

	if err := goose.UpContext(ctx, db, *dir); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	_, err = fmt.Fprintln(out, "migrations applied")
	return err
}
