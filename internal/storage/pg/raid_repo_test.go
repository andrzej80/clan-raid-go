package pg_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

const migrationsDir = "../../../migrations"

// testPool общий для всех тестов пакета; nil, если Docker недоступен.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("raidboss"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		// В CI Docker обязателен: иначе интеграционные тесты молча пропустятся, а сборка останется зелёной.
		if os.Getenv("REQUIRE_DOCKER") == "true" {
			fmt.Fprintln(os.Stderr, "postgres container unavailable, but REQUIRE_DOCKER=true:", err)
			return 1
		}
		fmt.Fprintln(os.Stderr, "postgres container unavailable, integration tests will be skipped:", err)
		return m.Run()
	}
	defer func() { _ = testcontainers.TerminateContainer(container) }()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection string:", err)
		return 1
	}
	testPool, err = pg.NewPool(ctx, &pg.DbConfig{Dsn: dsn, MaxConns: 20})
	if err != nil {
		fmt.Fprintln(os.Stderr, "new pool:", err)
		return 1
	}
	defer testPool.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		fmt.Fprintln(os.Stderr, "goose dialect:", err)
		return 1
	}
	db := stdlib.OpenDBFromPool(testPool)
	if err := goose.Up(db, migrationsDir); err != nil {
		fmt.Fprintln(os.Stderr, "migrations:", err)
		return 1
	}
	return m.Run()
}

func requirePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testPool == nil {
		t.Skip("docker is not available")
	}
	return testPool
}

func createClan(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		`INSERT INTO clans (name) VALUES ($1) RETURNING id`, t.Name()).Scan(&id)
	if err != nil {
		t.Fatalf("create clan: %v", err)
	}
	return id
}

func newRaid(clanID int64) *domain.Raid {
	// TIMESTAMPTZ хранит микросекунды.
	now := time.Now().UTC().Truncate(time.Microsecond)
	return &domain.Raid{
		ID:        uuid.New(),
		ClanID:    clanID,
		BossName:  "Dragon",
		HPMax:     1000,
		HPLeft:    1000,
		Status:    domain.RaidActive,
		StartedAt: now,
		EndsAt:    now.Add(time.Hour),
	}
}

func TestRaidRepo_CreateAndGet(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewRaidRepo(pool)
	ctx := context.Background()

	want := newRaid(createClan(t, pool))
	if err := repo.CreateRaid(ctx, want); err != nil {
		t.Fatalf("CreateRaid: %v", err)
	}

	got, err := repo.GetRaid(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetRaid: %v", err)
	}
	if got.ID != want.ID || got.ClanID != want.ClanID || got.BossName != want.BossName ||
		got.HPMax != want.HPMax || got.HPLeft != want.HPLeft || got.Status != want.Status ||
		!got.StartedAt.Equal(want.StartedAt) || !got.EndsAt.Equal(want.EndsAt) ||
		got.FinishedAt != nil || got.KillerPlayerID != nil || got.KillerHitID != nil {
		t.Fatalf("raid mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestRaidRepo_GetNotFound(t *testing.T) {
	t.Parallel()

	repo := pg.NewRaidRepo(requirePool(t))

	_, err := repo.GetRaid(context.Background(), uuid.New())
	if !errors.Is(err, domain.ErrRaidNotFound) {
		t.Fatalf("err = %v, want ErrRaidNotFound", err)
	}
}

func TestRaidRepo_CreateUnknownClan(t *testing.T) {
	t.Parallel()

	repo := pg.NewRaidRepo(requirePool(t))

	err := repo.CreateRaid(context.Background(), newRaid(-1))
	if !errors.Is(err, domain.ErrClanNotFound) {
		t.Fatalf("err = %v, want ErrClanNotFound", err)
	}
}

func TestRaidRepo_SecondActiveRaidRejected(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewRaidRepo(pool)
	ctx := context.Background()
	clanID := createClan(t, pool)

	if err := repo.CreateRaid(ctx, newRaid(clanID)); err != nil {
		t.Fatalf("first CreateRaid: %v", err)
	}
	err := repo.CreateRaid(ctx, newRaid(clanID))
	if !errors.Is(err, domain.ErrActiveRaidExists) {
		t.Fatalf("err = %v, want ErrActiveRaidExists", err)
	}
}

// Критерий из README: 50 горутин на один клан — 1 успех и 49 ErrActiveRaidExists.
func TestRaidRepo_ConcurrentCreateSameClan(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewRaidRepo(pool)
	clanID := createClan(t, pool)

	const workers = 50
	var (
		wg       sync.WaitGroup
		ok       atomic.Int32
		conflict atomic.Int32
		other    atomic.Int32
	)
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			switch err := repo.CreateRaid(context.Background(), newRaid(clanID)); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrActiveRaidExists):
				conflict.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	if ok.Load() != 1 || conflict.Load() != workers-1 || other.Load() != 0 {
		t.Fatalf("ok=%d conflict=%d other=%d, want 1/%d/0", ok.Load(), conflict.Load(), other.Load(), workers-1)
	}
}

func TestTxManager_RollbackOnError(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewRaidRepo(pool)
	txm := pg.NewTxManager(pool)
	ctx := context.Background()

	raid := newRaid(createClan(t, pool))
	boom := errors.New("boom")
	err := txm.WithinTx(ctx, func(ctx context.Context) error {
		if err := repo.CreateRaid(ctx, raid); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if _, err := repo.GetRaid(ctx, raid.ID); !errors.Is(err, domain.ErrRaidNotFound) {
		t.Fatalf("raid must be rolled back, GetRaid err = %v", err)
	}
}

func TestTxManager_CommitOnSuccess(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	repo := pg.NewRaidRepo(pool)
	txm := pg.NewTxManager(pool)
	ctx := context.Background()

	raid := newRaid(createClan(t, pool))
	if err := txm.WithinTx(ctx, func(ctx context.Context) error {
		return repo.CreateRaid(ctx, raid)
	}); err != nil {
		t.Fatalf("WithinTx: %v", err)
	}
	if _, err := repo.GetRaid(ctx, raid.ID); err != nil {
		t.Fatalf("raid must be committed: %v", err)
	}
}
