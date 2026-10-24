// botsim — нагрузочный симулятор игроков: gRPC-клиент, каждый игрок — горутина.
// Это минимальная версия для проверки консьюмера (этап 3); полноценный open-model режим — этап 8.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func main() {
	ctx, stop := app.SignalContext()
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("botsim", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:9090", "api gRPC address")
	duration := fs.Duration("duration", 30*time.Second, "how long to attack")
	cfg := simConfig{}
	fs.IntVar(&cfg.clans, "clans", 5, "number of clans (raids)")
	fs.IntVar(&cfg.players, "players", 50, "players per clan")
	fs.Float64Var(&cfg.rate, "rate", 2, "attacks per second per player (server limit is 5)")
	fs.Float64Var(&cfg.dupProb, "dup-prob", 0.05, "probability to resend an accepted hit with the same idempotency key")
	fs.Int64Var(&cfg.hp, "hp", 300_000, "boss hp for newly created raids")
	fs.DurationVar(&cfg.raidDuration, "raid-duration", 10*time.Minute, "lifetime of newly created raids")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.rate <= 0 || cfg.clans <= 0 || cfg.players <= 0 {
		return errors.New("rate, clans and players must be positive")
	}

	appCfg, err := config.Load() // нужен DATABASE_URL: игроков и активные рейды бот берёт из БД
	if err != nil {
		return err
	}
	pool, err := pg.NewPool(ctx, &appCfg.DB)
	if err != nil {
		return err
	}
	defer pool.Close()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Warn("close grpc conn", "err", err)
		}
	}()

	runCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	start := time.Now()
	st, raidIDs, err := simulate(runCtx, pool, conn, cfg)
	if err != nil {
		return err
	}
	fmt.Print(st.format(time.Since(start)))
	fmt.Println("raids:")
	for _, id := range raidIDs {
		fmt.Println(" ", id)
	}
	return nil
}
