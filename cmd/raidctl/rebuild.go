package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/rebuild"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
)

func runRebuildCache(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("rebuild-cache", flag.ContinueOnError)
	raidID := fs.String("raid", "", "rebuild only this raid (default: all raids)")
	chunk := fs.Int("chunk", 0, "players per ZADD (default 1000)")
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
	if cfg.Redis.URL == "" {
		return errors.New("REDIS_URL is required")
	}
	pool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer pool.Close()
	redisClient, err := rdb.New(ctx, &cfg.Redis)
	if err != nil {
		return err
	}
	defer func() { _ = redisClient.Close() }() //nolint:gosec // закрытие при выходе, ошибку обработать нечем

	start := time.Now()
	res, err := rebuild.Run(ctx, pool, redisClient.Redis(), raidIDs, rebuild.Options{ChunkSize: *chunk})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "rebuilt %d raids, %d leaderboard entries in %s\n",
		res.Raids, res.Players, time.Since(start).Round(time.Millisecond))
	return err
}
