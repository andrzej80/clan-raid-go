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
	conns := fs.Int("conns", 1, "number of gRPC connections (separate TCP), calls are spread round-robin")
	fs.IntVar(&cfg.watchers, "watchers", 0, "WatchRaid subscribers, spread over the raids round-robin")
	jsonPath := fs.String("json", "", "write the report as JSON to this file")
	mode := fs.String("mode", "closed", "closed: each player attacks at --rate; open: scheduler fires --target-rps regardless of responses")
	fs.Float64Var(&cfg.targetRPS, "target-rps", 200, "open mode: total attacks per second")
	fs.DurationVar(&cfg.rampUp, "ramp-up", 0, "open mode: linear ramp from 0 to --target-rps")
	fs.IntVar(&cfg.maxInflight, "max-inflight", 2000, "open mode: cap on concurrent requests; excess requests are dropped and counted")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := applyMode(&cfg, *mode); err != nil {
		return err
	}
	if cfg.rate <= 0 || cfg.clans <= 0 || cfg.players <= 0 || cfg.watchers < 0 {
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

	conn, err := newConnPool(*addr, *conns)
	if err != nil {
		return err
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
	rep := st.report(time.Since(start))
	rep.Conns = *conns
	fmt.Print(rep.format())
	fmt.Println("raids:")
	for _, id := range raidIDs {
		fmt.Println(" ", id)
	}
	return writeJSON(*jsonPath, rep)
}

// writeJSON сохраняет отчёт в файл; пустой путь означает «не сохранять».
func writeJSON(path string, rep report) error {
	if path == "" {
		return nil
	}
	data, err := rep.json()
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

// applyMode разбирает флаг --mode и проверяет параметры open model.
func applyMode(cfg *simConfig, mode string) error {
	switch mode {
	case "closed":
		return nil
	case "open":
		cfg.open = true
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	if cfg.targetRPS <= 0 || cfg.maxInflight <= 0 {
		return errors.New("target-rps and max-inflight must be positive")
	}
	if cfg.clans > 0 && cfg.players > 0 {
		if per := cfg.targetRPS / float64(cfg.clans*cfg.players); per > 5 {
			slog.Warn("per-player rate exceeds the server limit of 5/s: expect RESOURCE_EXHAUSTED; raise --players or --clans",
				"per_player_rps", per)
		}
	}
	return nil
}
