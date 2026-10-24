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
	"github.com/andrzej80/clan-raid-go/internal/verify"
)

var errInvariantsViolated = errors.New("invariants violated")

func runVerify(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	raidID := fs.String("raid", "", "check only this raid (default: all raids)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var raidIDs []uuid.UUID
	if *raidID != "" {
		id, err := uuid.Parse(*raidID)
		if err != nil {
			return fmt.Errorf("invalid --raid: %w", err)
		}
		raidIDs = []uuid.UUID{id}
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

	violations, err := verify.Run(ctx, pool, raidIDs)
	if err != nil {
		return err
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
