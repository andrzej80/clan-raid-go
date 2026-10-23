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

var testNow = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

// countingRaids и countingPlayers считают обращения к «БД».
type countingRaids struct {
	raid  *domain.Raid
	calls atomic.Int32
	delay time.Duration
}

func (r *countingRaids) CreateRaid(context.Context, *domain.Raid) error { return nil }

func (r *countingRaids) GetRaid(_ context.Context, id uuid.UUID) (*domain.Raid, error) {
	r.calls.Add(1)
	<-time.After(r.delay)
	if r.raid == nil || r.raid.ID != id {
		return nil, domain.ErrRaidNotFound
	}
	cp := *r.raid
	return &cp, nil
}

type countingPlayers struct {
	player *domain.Player
	calls  atomic.Int32
	delay  time.Duration
}

func (p *countingPlayers) GetPlayer(_ context.Context, id int64) (*domain.Player, error) {
	p.calls.Add(1)
	<-time.After(p.delay)
	if p.player == nil || p.player.ID != id {
		return nil, domain.ErrPlayerNotFound
	}
	cp := *p.player
	return &cp, nil
}

type fakePublisher struct {
	mu   sync.Mutex
	hits []domain.Hit
	err  error
}

func (f *fakePublisher) Publish(_ context.Context, hit domain.Hit) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.hits = append(f.hits, hit)
	return nil
}

func (f *fakePublisher) published() []domain.Hit {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.Hit(nil), f.hits...)
}

type attackEnv struct {
	svc       *AttackService
	raids     *countingRaids
	players   *countingPlayers
	publisher *fakePublisher
	params    AttackParams
}

func newAttackEnv() *attackEnv {
	raid := &domain.Raid{
		ID: uuid.New(), ClanID: 1, BossName: "Dragon", HPMax: 1000, HPLeft: 1000,
		Status: domain.RaidActive, StartedAt: testNow.Add(-time.Hour), EndsAt: testNow.Add(time.Hour),
	}
	player := &domain.Player{ID: 10, ClanID: 1, Name: "p", Power: 500}
	env := &attackEnv{
		raids:     &countingRaids{raid: raid},
		players:   &countingPlayers{player: player},
		publisher: &fakePublisher{},
		params:    AttackParams{RaidID: raid.ID, PlayerID: player.ID, IdempotencyKey: uuid.New()},
	}
	env.svc = NewAttackService(env.raids, env.players, env.publisher, WithClock(func() time.Time { return testNow }))
	return env
}

func TestAttack_Success(t *testing.T) {
	t.Parallel()
	env := newAttackEnv()

	res, err := env.svc.Attack(context.Background(), env.params)
	if err != nil {
		t.Fatalf("Attack: %v", err)
	}

	hits := env.publisher.published()
	if len(hits) != 1 {
		t.Fatalf("published %d hits, want 1", len(hits))
	}
	hit := hits[0]
	if hit.ID != env.params.IdempotencyKey || hit.RaidID != env.params.RaidID ||
		hit.PlayerID != 10 || hit.ClanID != 1 || !hit.CreatedAt.Equal(testNow) {
		t.Fatalf("unexpected hit: %+v", hit)
	}
	if res.HitID != hit.ID || res.Damage != hit.Damage || res.IsCrit != hit.IsCrit {
		t.Fatalf("response %+v does not match published hit %+v", res, hit)
	}

	lo, hi := 400, 600 // power 500, u ∈ [0.8, 1.2)
	if hit.IsCrit {
		lo, hi = 800, 1200
	}
	if hit.Damage < lo || hit.Damage > hi {
		t.Fatalf("damage %d (crit=%t) out of [%d, %d]", hit.Damage, hit.IsCrit, lo, hi)
	}
}

// Повтор с тем же ключом возвращает тот же удар: клиент не видит одно, а засчитывается другое.
func TestAttack_RetryWithSameKeyGivesSameHit(t *testing.T) {
	t.Parallel()
	env := newAttackEnv()
	ctx := context.Background()

	first, err := env.svc.Attack(ctx, env.params)
	if err != nil {
		t.Fatalf("first Attack: %v", err)
	}
	second, err := env.svc.Attack(ctx, env.params)
	if err != nil {
		t.Fatalf("second Attack: %v", err)
	}

	if first != second {
		t.Fatalf("retry returned %+v, first was %+v", second, first)
	}
	hits := env.publisher.published()
	if len(hits) != 2 || hits[0] != hits[1] {
		t.Fatalf("retry must publish an identical event (consumer dedups by hit_id): %+v", hits)
	}
}

func TestAttack_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*AttackParams)
	}{
		{"no player", func(p *AttackParams) { p.PlayerID = 0 }},
		{"negative player", func(p *AttackParams) { p.PlayerID = -1 }},
		{"no raid", func(p *AttackParams) { p.RaidID = uuid.Nil }},
		{"no idempotency key", func(p *AttackParams) { p.IdempotencyKey = uuid.Nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newAttackEnv()
			tc.mutate(&env.params)

			_, err := env.svc.Attack(context.Background(), env.params)
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
			if env.players.calls.Load()+env.raids.calls.Load() != 0 {
				t.Fatal("invalid request must not touch the database")
			}
		})
	}
}

func TestAttack_Rejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*attackEnv)
		want   error
	}{
		{"unknown player", func(e *attackEnv) { e.params.PlayerID = 999 }, domain.ErrPlayerNotFound},
		{"unknown raid", func(e *attackEnv) { e.params.RaidID = uuid.New() }, domain.ErrRaidNotFound},
		{"another clan", func(e *attackEnv) { e.players.player.ClanID = 2 }, domain.ErrWrongClan},
		{"defeated raid", func(e *attackEnv) { e.raids.raid.Status = domain.RaidDefeated }, domain.ErrRaidNotActive},
		{"expired status", func(e *attackEnv) { e.raids.raid.Status = domain.RaidExpired }, domain.ErrRaidNotActive},
		{"active but past ends_at", func(e *attackEnv) { e.raids.raid.EndsAt = testNow }, domain.ErrRaidNotActive},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newAttackEnv()
			tc.mutate(env)

			_, err := env.svc.Attack(context.Background(), env.params)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if n := len(env.publisher.published()); n != 0 {
				t.Fatalf("rejected attack must not be published, got %d hits", n)
			}
		})
	}
}

func TestAttack_PublishErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pub  error
		want error
	}{
		{"broker down", errors.New("kafka: connection refused 10.0.0.5"), domain.ErrQueueUnavailable},
		{"client deadline", context.DeadlineExceeded, context.DeadlineExceeded},
		{"client cancel", context.Canceled, context.Canceled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newAttackEnv()
			env.publisher.err = tc.pub

			_, err := env.svc.Attack(context.Background(), env.params)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if errors.Is(tc.want, domain.ErrQueueUnavailable) && errors.Is(err, tc.pub) {
				t.Fatal("broker details must not leak into the returned error")
			}
		})
	}
}

// Кэш: игрок живёт минуту, рейд секунду.
func TestAttack_CacheTTL(t *testing.T) {
	t.Parallel()
	env := newAttackEnv()
	clock := testNow
	env.svc = NewAttackService(env.raids, env.players, env.publisher, WithClock(func() time.Time { return clock }))
	ctx := context.Background()

	for range 3 {
		if _, err := env.svc.Attack(ctx, env.params); err != nil {
			t.Fatalf("Attack: %v", err)
		}
	}
	if env.players.calls.Load() != 1 || env.raids.calls.Load() != 1 {
		t.Fatalf("within TTL: players=%d raids=%d, want 1/1", env.players.calls.Load(), env.raids.calls.Load())
	}

	clock = clock.Add(1100 * time.Millisecond) // рейд протух, игрок нет
	if _, err := env.svc.Attack(ctx, env.params); err != nil {
		t.Fatalf("Attack: %v", err)
	}
	if env.players.calls.Load() != 1 || env.raids.calls.Load() != 2 {
		t.Fatalf("after 1.1s: players=%d raids=%d, want 1/2", env.players.calls.Load(), env.raids.calls.Load())
	}

	// Убитый рейд подхватывается после протухания кэша.
	env.raids.raid.Status = domain.RaidDefeated
	clock = clock.Add(1100 * time.Millisecond)
	if _, err := env.svc.Attack(ctx, env.params); !errors.Is(err, domain.ErrRaidNotActive) {
		t.Fatalf("err = %v, want ErrRaidNotActive", err)
	}
}

// Критерий этапа 2: при промахе кэша 1000 параллельных запросов делают 1 запрос в БД.
func TestAttack_ConcurrentColdCacheHitsDBOnce(t *testing.T) {
	t.Parallel()
	env := newAttackEnv()
	env.raids.delay = 20 * time.Millisecond
	env.players.delay = 20 * time.Millisecond

	runConcurrentAttacks(t, env, 1000)

	if env.raids.calls.Load() != 1 || env.players.calls.Load() != 1 {
		t.Fatalf("DB calls: raids=%d players=%d, want 1/1", env.raids.calls.Load(), env.players.calls.Load())
	}
	if n := len(env.publisher.published()); n != 1000 {
		t.Fatalf("published %d hits, want 1000", n)
	}
}

func BenchmarkAttack_ConcurrentColdCache(b *testing.B) {
	var raidCalls, playerCalls int64
	for range b.N {
		env := newAttackEnv()
		env.raids.delay = 5 * time.Millisecond
		env.players.delay = 5 * time.Millisecond
		runConcurrentAttacks(b, env, 1000)
		raidCalls += int64(env.raids.calls.Load())
		playerCalls += int64(env.players.calls.Load())
	}
	b.ReportMetric(float64(raidCalls)/float64(b.N), "raid-db-calls/op")
	b.ReportMetric(float64(playerCalls)/float64(b.N), "player-db-calls/op")
}

func runConcurrentAttacks(tb testing.TB, env *attackEnv, n int) {
	tb.Helper()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range n {
		params := env.params
		params.IdempotencyKey = uuid.New()
		wg.Go(func() {
			<-start
			if _, err := env.svc.Attack(context.Background(), params); err != nil {
				tb.Errorf("Attack: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
}
