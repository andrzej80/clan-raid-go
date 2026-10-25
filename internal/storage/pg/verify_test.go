package pg_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/verify"
)

func verifyRaid(t *testing.T, env *processorEnv, raidID uuid.UUID) []verify.Violation {
	t.Helper()

	v, err := verify.Run(context.Background(), env.pool, []uuid.UUID{raidID})
	if err != nil {
		t.Fatalf("verify.Run: %v", err)
	}
	return v
}

func invariants(vs []verify.Violation) map[int]bool {
	out := make(map[int]bool, len(vs))
	for _, v := range vs {
		out[v.Invariant] = true
	}
	return out
}

func TestVerify_ConsistentRaidHasNoViolations(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := env.newRaidWithHP(t, 100)
	mustProcess(t, env.processor, []domain.Hit{
		env.hit(r.ID, env.players[0], 60),
		env.hit(r.ID, env.players[1], 50), // killing blow
		env.hit(r.ID, env.players[0], 10), // опоздал
	})

	if v := verifyRaid(t, env, r.ID); len(v) != 0 {
		t.Fatalf("violations on consistent data: %v", v)
	}
}

func TestVerify_ActiveRaidHasNoViolations(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 1000)
	mustProcess(t, env.processor, []domain.Hit{env.hit(r.ID, env.players[0], 100)})

	if v := verifyRaid(t, env, r.ID); len(v) != 0 {
		t.Fatalf("violations on consistent data: %v", v)
	}
}

func TestVerify_DetectsCorruption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		corrupt func(t *testing.T, env *processorEnv, raidID uuid.UUID, hits []domain.Hit)
		want    int
	}{
		{
			name: "hit accepted after killing blow",
			corrupt: func(t *testing.T, env *processorEnv, raidID uuid.UUID, _ []domain.Hit) {
				t.Helper()
				late := env.hit(raidID, env.players[0], 5)
				env.exec(t, `INSERT INTO raid_hits (hit_id, raid_id, player_id, damage, is_crit, created_at)
					VALUES ($1, $2, $3, 5, false, now())`, late.ID, raidID, late.PlayerID)
			},
			want: 3,
		},
		{
			name: "raid_damage differs from raid_hits",
			corrupt: func(t *testing.T, env *processorEnv, raidID uuid.UUID, _ []domain.Hit) {
				t.Helper()
				env.exec(t, `UPDATE raid_damage SET total_damage = total_damage + 1 WHERE raid_id = $1`, raidID)
			},
			want: 4,
		},
		{
			name: "defeated raid without killer hit",
			corrupt: func(t *testing.T, env *processorEnv, raidID uuid.UUID, _ []domain.Hit) {
				t.Helper()
				env.exec(t, `UPDATE raids SET killer_hit_id = NULL WHERE id = $1`, raidID)
			},
			want: 2,
		},
		{
			name: "hits do not cover hp_max",
			corrupt: func(t *testing.T, env *processorEnv, raidID uuid.UUID, hits []domain.Hit) {
				t.Helper()
				env.exec(t, `UPDATE raid_hits SET damage = 1 WHERE hit_id = $1`, hits[0].ID)
			},
			want: 1,
		},
		{
			name: "hp_left out of sync",
			corrupt: func(t *testing.T, env *processorEnv, raidID uuid.UUID, _ []domain.Hit) {
				t.Helper()
				env.exec(t, `UPDATE raids SET hp_left = 7 WHERE id = $1`, raidID)
			},
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newProcessorEnv(t, 1)
			r := env.newRaidWithHP(t, 100)
			hits := []domain.Hit{env.hit(r.ID, env.players[0], 60), env.hit(r.ID, env.players[0], 60)}
			mustProcess(t, env.processor, hits)
			if v := verifyRaid(t, env, r.ID); len(v) != 0 {
				t.Fatalf("setup is inconsistent: %v", v)
			}

			tc.corrupt(t, env, r.ID, hits)

			if got := invariants(verifyRaid(t, env, r.ID)); !got[tc.want] {
				t.Fatalf("invariant %d not reported, got %v", tc.want, got)
			}
		})
	}
}

func verifyRewards(t *testing.T, env *processorEnv, raidID uuid.UUID, requireAll bool) []verify.Violation {
	t.Helper()

	v, err := verify.RunRewards(context.Background(), env.pool, []uuid.UUID{raidID}, requireAll)
	if err != nil {
		t.Fatalf("verify.RunRewards: %v", err)
	}
	return v
}

func TestVerify_RewardsConsistentAfterWorker(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := defeatedRaid(t, env)
	if _, err := newRewardsProcessor(t, env).ProcessEvents(context.Background(),
		[]domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventDefeated}}); err != nil {
		t.Fatalf("ProcessEvents: %v", err)
	}

	if v := verifyRaid(t, env, r.ID); len(v) != 0 {
		t.Fatalf("Run violations: %v", v)
	}
	if v := verifyRewards(t, env, r.ID, true); len(v) != 0 {
		t.Fatalf("RunRewards violations: %v", v)
	}
}

// До работы rewards-worker отсутствие наград — не ошибка, пока не включён requireAll.
func TestVerify_RewardsNotYetGranted(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := defeatedRaid(t, env)

	if v := verifyRewards(t, env, r.ID, false); len(v) != 0 {
		t.Fatalf("violations without requireAll: %v", v)
	}
	if v := verifyRewards(t, env, r.ID, true); len(v) != 1 || v[0].Invariant != 0 {
		t.Fatalf("violations with requireAll = %v, want one 'no rewards'", v)
	}
}

func TestVerify_DetectsRewardCorruption(t *testing.T) {
	t.Parallel()

	grant := func(t *testing.T) (*processorEnv, *domain.Raid) {
		t.Helper()
		env := newProcessorEnv(t, 2)
		r := defeatedRaid(t, env)
		if _, err := newRewardsProcessor(t, env).ProcessEvents(context.Background(),
			[]domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventDefeated}}); err != nil {
			t.Fatalf("ProcessEvents: %v", err)
		}
		return env, r
	}

	t.Run("wrong amount", func(t *testing.T) {
		t.Parallel()
		env, r := grant(t)
		env.exec(t, `UPDATE rewards SET amount = amount + 1 WHERE raid_id = $1 AND kind = 'top3' AND player_id = $2`,
			r.ID, env.players[0])

		if v := verifyRewards(t, env, r.ID, false); !invariants(v)[5] {
			t.Fatalf("wrong amount not detected: %v", v)
		}
	})
	t.Run("missing reward", func(t *testing.T) {
		t.Parallel()
		env, r := grant(t)
		env.exec(t, `DELETE FROM rewards WHERE raid_id = $1 AND kind = 'killing_blow'`, r.ID)

		if v := verifyRewards(t, env, r.ID, false); !invariants(v)[5] {
			t.Fatalf("missing reward not detected: %v", v)
		}
	})
	t.Run("extra reward", func(t *testing.T) {
		t.Parallel()
		env, r := grant(t)
		env.exec(t, `INSERT INTO rewards (raid_id, player_id, kind, amount) VALUES ($1, $2, 'killing_blow', 250)`,
			r.ID, env.players[1])

		if v := verifyRewards(t, env, r.ID, false); !invariants(v)[5] {
			t.Fatalf("extra reward not detected: %v", v)
		}
	})
	t.Run("reward for a raid that is not defeated", func(t *testing.T) {
		t.Parallel()
		env := newProcessorEnv(t, 1)
		active := env.newRaidWithHP(t, 100)
		env.exec(t, `INSERT INTO rewards (raid_id, player_id, kind, amount) VALUES ($1, $2, 'participation', 100)`,
			active.ID, env.players[0])

		if v := verifyRaid(t, env, active.ID); !invariants(v)[5] {
			t.Fatalf("reward for active raid not detected: %v", v)
		}
	})
}
