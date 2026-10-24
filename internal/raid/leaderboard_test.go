package raid

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type fakeBoard struct {
	entries []domain.LeaderboardEntry
	err     error
	limit   atomic.Int64
	calls   atomic.Int32
}

func (b *fakeBoard) Top(_ context.Context, _ uuid.UUID, limit int) ([]domain.LeaderboardEntry, error) {
	b.calls.Add(1)
	b.limit.Store(int64(limit))
	return b.entries, b.err
}

type fakeDamage struct {
	entries []domain.LeaderboardEntry
	err     error
	calls   atomic.Int32
	release chan struct{} // если задан, TopDamage ждёт закрытия
}

func (d *fakeDamage) TopDamage(_ context.Context, _ uuid.UUID, _ int) ([]domain.LeaderboardEntry, error) {
	d.calls.Add(1)
	if d.release != nil {
		<-d.release
	}
	return d.entries, d.err
}

type raidByID map[uuid.UUID]*domain.Raid

func (r raidByID) CreateRaid(context.Context, *domain.Raid) error { return nil }
func (r raidByID) GetRaid(_ context.Context, id uuid.UUID) (*domain.Raid, error) {
	if raid, ok := r[id]; ok {
		return raid, nil
	}
	return nil, domain.ErrRaidNotFound
}

var (
	redisTop = []domain.LeaderboardEntry{{PlayerID: 1, TotalDamage: 300}, {PlayerID: 2, TotalDamage: 100}}
	pgTop    = []domain.LeaderboardEntry{{PlayerID: 9, TotalDamage: 999}}
)

type lbEnv struct {
	svc    *LeaderboardService
	board  *fakeBoard
	damage *fakeDamage
	raidID uuid.UUID
}

// newLBEnv создаёт рейд с hp_left = hpLeft из 1000.
func newLBEnv(t *testing.T, hpLeft int64) *lbEnv {
	t.Helper()
	raidID := uuid.New()
	env := &lbEnv{
		board:  &fakeBoard{entries: redisTop},
		damage: &fakeDamage{entries: pgTop},
		raidID: raidID,
	}
	repo := raidByID{raidID: {ID: raidID, HPMax: 1000, HPLeft: hpLeft, Status: domain.RaidActive}}
	svc, err := NewLeaderboardService(repo, env.board, env.damage)
	if err != nil {
		t.Fatalf("NewLeaderboardService: %v", err)
	}
	env.svc = svc
	return env
}

func TestLeaderboard_ServedFromRedis(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 500)

	got, err := env.svc.Top(context.Background(), env.raidID, 5)
	if err != nil || len(got) != 2 || got[0].PlayerID != 1 {
		t.Fatalf("top = %+v, err = %v", got, err)
	}
	if env.damage.calls.Load() != 0 {
		t.Fatal("postgres must not be touched while redis answers")
	}
}

func TestLeaderboard_LimitNormalization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		limit     int
		wantLimit int64
		wantErr   bool
	}{
		{"zero means default", 0, 10, false},
		{"explicit", 25, 25, false},
		{"clamped to max", 5000, 100, false},
		{"negative is invalid", -1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := newLBEnv(t, 500)
			_, err := env.svc.Top(context.Background(), env.raidID, tt.limit)
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("err = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Top: %v", err)
			}
			if got := env.board.limit.Load(); got != tt.wantLimit {
				t.Fatalf("limit passed to redis = %d, want %d", got, tt.wantLimit)
			}
		})
	}
}

func TestLeaderboard_UnknownRaid(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 500)

	_, err := env.svc.Top(context.Background(), uuid.New(), 10)
	if !errors.Is(err, domain.ErrRaidNotFound) {
		t.Fatalf("err = %v, want ErrRaidNotFound", err)
	}
	if env.board.calls.Load() != 0 {
		t.Fatal("redis must not be queried for an unknown raid")
	}
}

func TestLeaderboard_RedisErrorFallsBackToPostgres(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 500)
	env.board.err = errors.New("redis down")

	got, err := env.svc.Top(context.Background(), env.raidID, 10)
	if err != nil || len(got) != 1 || got[0].PlayerID != 9 {
		t.Fatalf("top = %+v, err = %v, want postgres data", got, err)
	}
}

// Redis сбросили: лидерборд пуст, хотя урон уже нанесён.
func TestLeaderboard_EmptyBoardOfDamagedRaidFallsBack(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 500)
	env.board.entries = nil

	got, err := env.svc.Top(context.Background(), env.raidID, 10)
	if err != nil || len(got) != 1 || got[0].PlayerID != 9 {
		t.Fatalf("top = %+v, err = %v, want postgres data", got, err)
	}
}

// Босса ещё никто не бил: пустой лидерборд — правда, а не потеря данных.
func TestLeaderboard_EmptyBoardOfPristineRaidIsAnswer(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 1000)
	env.board.entries = nil

	got, err := env.svc.Top(context.Background(), env.raidID, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("top = %+v, err = %v, want empty", got, err)
	}
	if env.damage.calls.Load() != 0 {
		t.Fatal("postgres must not be queried for a raid nobody hit")
	}
}

// Главный вопрос фоллбэка: не завалит ли он базу. 1000 одновременных чтений при мёртвом Redis
// дают один запрос в Postgres, а повторные в течение TTL — ни одного.
func TestLeaderboard_FallbackLoadIsCollapsed(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 500)
	env.board.err = errors.New("redis down")
	env.damage.release = make(chan struct{})

	var wg sync.WaitGroup
	for range 1000 {
		wg.Go(func() {
			if _, err := env.svc.Top(context.Background(), env.raidID, 10); err != nil {
				t.Errorf("Top: %v", err)
			}
		})
	}
	for env.damage.calls.Load() == 0 { // дождаться, пока первая загрузка начнётся
		time.Sleep(time.Millisecond) //nolint:forbidigo // ожидание старта в тесте
	}
	close(env.damage.release)
	wg.Wait()

	if _, err := env.svc.Top(context.Background(), env.raidID, 10); err != nil {
		t.Fatalf("Top: %v", err)
	}
	if got := env.damage.calls.Load(); got != 1 {
		t.Fatalf("postgres calls = %d, want 1", got)
	}
}

func TestLeaderboard_FallbackCacheExpires(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	raidID := uuid.New()
	board := &fakeBoard{err: errors.New("redis down")}
	damage := &fakeDamage{entries: pgTop}
	repo := raidByID{raidID: {ID: raidID, HPMax: 1000, HPLeft: 500}}
	svc, err := NewLeaderboardService(repo, board, damage, WithLeaderboardClock(clock))
	if err != nil {
		t.Fatalf("NewLeaderboardService: %v", err)
	}

	for range 3 {
		_, _ = svc.Top(context.Background(), raidID, 10)
	}
	mu.Lock()
	now = now.Add(leaderboardFallbackTTL)
	mu.Unlock()
	_, _ = svc.Top(context.Background(), raidID, 10)

	if got := damage.calls.Load(); got != 2 {
		t.Fatalf("postgres calls = %d, want 2 (one per TTL window)", got)
	}
}

func TestLeaderboard_PostgresErrorIsReturned(t *testing.T) {
	t.Parallel()
	env := newLBEnv(t, 500)
	env.board.err = errors.New("redis down")
	env.damage.err = errors.New("pg down")

	if _, err := env.svc.Top(context.Background(), env.raidID, 10); err == nil {
		t.Fatal("expected an error when both redis and postgres fail")
	}
}
