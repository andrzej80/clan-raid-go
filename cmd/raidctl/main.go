// raidctl — служебный CLI: migrate, seed, verify, rebuild-cache, outbox dump.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/seed"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

const usage = `usage: raidctl <command> [flags]

commands:
  migrate apply SQL migrations (raidctl migrate --dir migrations)
  seed    create clans and players (raidctl seed --clans 100 --players 50)
  verify  check invariants in PostgreSQL and Redis (raidctl verify [--raid <uuid>] [--redis=false] [--require-rewards]);
          exit code 1 on violations
  rebuild-cache
          rebuild the Redis leaderboard and hp cache from PostgreSQL (raidctl rebuild-cache [--raid <uuid>])
  outbox dump
          print outbox records as JSON lines, payload decoded (raidctl outbox dump [--unsent] [--topic t] [--key k] [--limit 50])
`

func main() {
	os.Exit(realMain())
}

// realMain отделён от main, чтобы defer stop() отработал до os.Exit.
func realMain() int {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("command is required\n\n%s", usage)
	}
	switch args[0] {
	case "migrate":
		return runMigrate(ctx, args[1:], out)
	case "verify":
		return runVerify(ctx, args[1:], out)
	case "rebuild-cache":
		return runRebuildCache(ctx, args[1:], out)
	case "seed":
		return runSeed(ctx, args[1:], out)
	case "outbox":
		return runOutbox(ctx, args[1:], out)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usage)
	}
}

func runSeed(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	clans := fs.Int("clans", 100, "number of clans to create")
	players := fs.Int("players", 50, "players per clan")
	randSeed := fs.Uint64("seed", uint64(time.Now().UnixNano()), "random seed for player power (for reproducible data)")
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

	rng := rand.New(rand.NewPCG(*randSeed, 0)) //nolint:gosec // тестовые данные; PCG нужен для воспроизводимости по --seed
	res, err := seed.Run(ctx, pg.NewCatalogRepo(pool), pg.NewTxManager(pool),
		seed.Options{Clans: *clans, PlayersPerClan: *players}, rng)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "created %d clans, %d players\n", res.Clans, res.Players)
	return err
}
