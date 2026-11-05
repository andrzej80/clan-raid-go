package pg_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/verify"
)

func TestSyncHitsRepo_AppliesAndKills(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	repo := pg.NewSyncHitsRepo(env.pool)
	r := env.newRaidWithHP(t, 100)
	ctx := context.Background()

	if err := repo.ApplyHit(ctx, env.hit(r.ID, env.players[0], 60)); err != nil {
		t.Fatalf("first hit: %v", err)
	}
	got, err := env.raids.GetRaid(ctx, r.ID)
	if err != nil || got.HPLeft != 40 || got.Status != domain.RaidActive {
		t.Fatalf("after first hit: %+v, %v; want hp 40, active", got, err)
	}

	killer := env.hit(r.ID, env.players[0], 70)
	if err := repo.ApplyHit(ctx, killer); err != nil {
		t.Fatalf("killing hit: %v", err)
	}
	got, err = env.raids.GetRaid(ctx, r.ID)
	if err != nil || got.HPLeft != 0 || got.Status != domain.RaidDefeated {
		t.Fatalf("after kill: %+v, %v; want hp 0, defeated", got, err)
	}

	// Удар по уже побеждённому рейду не записывается.
	err = repo.ApplyHit(ctx, env.hit(r.ID, env.players[0], 10))
	if !errors.Is(err, domain.ErrRaidNotActive) {
		t.Fatalf("hit after kill: err = %v, want ErrRaidNotActive", err)
	}
	if n := env.count(t, `SELECT count(*) FROM raid_hits WHERE raid_id = $1`, r.ID); n != 2 {
		t.Fatalf("raid_hits = %d, want 2", n)
	}
	assertSyncInvariants(t, env, r.ID)
}

func TestSyncHitsRepo_DuplicateDoesNotDoubleDamage(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	repo := pg.NewSyncHitsRepo(env.pool)
	r := env.newRaidWithHP(t, 100)
	ctx := context.Background()

	h := env.hit(r.ID, env.players[0], 30)
	for range 3 {
		if err := repo.ApplyHit(ctx, h); err != nil {
			t.Fatalf("ApplyHit: %v", err)
		}
	}
	got, err := env.raids.GetRaid(ctx, r.ID)
	if err != nil || got.HPLeft != 70 {
		t.Fatalf("hp_left = %d (%v), want 70: duplicate must not be applied twice", got.HPLeft, err)
	}
}

func TestSyncHitsRepo_UnknownPlayerRollsBackHP(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 0)
	repo := pg.NewSyncHitsRepo(env.pool)
	r := env.newRaidWithHP(t, 100)

	err := repo.ApplyHit(context.Background(), env.hit(r.ID, -1, 30))
	if !errors.Is(err, domain.ErrPlayerNotFound) {
		t.Fatalf("err = %v, want ErrPlayerNotFound", err)
	}
	got, err := env.raids.GetRaid(context.Background(), r.ID)
	if err != nil || got.HPLeft != 100 {
		t.Fatalf("hp_left = %d (%v), want 100: failed hit must not change HP", got.HPLeft, err)
	}
}

// 50 горутин бьют одного босса: HP сходится, killing blow ровно один, удары после него не принимаются.
func TestSyncHitsRepo_ConcurrentHitsOneKiller(t *testing.T) {
	t.Parallel()

	const workers, damage, hp = 50, 10, 300
	env := newProcessorEnv(t, 1)
	repo := pg.NewSyncHitsRepo(env.pool)
	r := env.newRaidWithHP(t, hp)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			err := repo.ApplyHit(context.Background(), env.hit(r.ID, env.players[0], damage))
			if err != nil && !errors.Is(err, domain.ErrRaidNotActive) {
				t.Errorf("ApplyHit: %v", err)
			}
		})
	}
	wg.Wait()

	got, err := env.raids.GetRaid(context.Background(), r.ID)
	if err != nil || got.HPLeft != 0 || got.Status != domain.RaidDefeated {
		t.Fatalf("raid = %+v, %v; want defeated with hp 0", got, err)
	}
	if n := env.count(t, `SELECT count(*) FROM raid_hits WHERE raid_id = $1`, r.ID); n != hp/damage {
		t.Fatalf("accepted hits = %d, want %d", n, hp/damage)
	}
	assertSyncInvariants(t, env, r.ID)
}

// sync-full — тот же HitsProcessor, пачка из одного удара: проходят все инварианты, включая raid_damage.
func TestProcessorApplier_SingleHitBatches(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	applier := raid.ProcessorApplier{Processor: env.processor}
	r := env.newRaidWithHP(t, 100)
	ctx := context.Background()

	for _, dmg := range []int{40, 40, 40, 40} { // последний удар опоздал: рейд уже побеждён
		if err := applier.ApplyHit(ctx, env.hit(r.ID, env.players[0], dmg)); err != nil {
			t.Fatalf("ApplyHit: %v", err)
		}
	}
	got, err := env.raids.GetRaid(ctx, r.ID)
	if err != nil || got.HPLeft != 0 || got.Status != domain.RaidDefeated {
		t.Fatalf("raid = %+v, %v; want defeated", got, err)
	}
	violations, err := verify.Run(ctx, env.pool, []uuid.UUID{r.ID})
	if err != nil || len(violations) != 0 {
		t.Fatalf("verify: %v, %v", violations, err)
	}
}

// assertSyncInvariants проверяет инварианты рейда, кроме 4: «голый» sync не ведёт raid_damage.
func assertSyncInvariants(t *testing.T, env *processorEnv, raidID uuid.UUID) {
	t.Helper()

	violations, err := verify.Run(context.Background(), env.pool, []uuid.UUID{raidID})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	for _, v := range violations {
		if v.Invariant == 4 {
			continue
		}
		t.Errorf("invariant violated: %s", v)
	}
}
