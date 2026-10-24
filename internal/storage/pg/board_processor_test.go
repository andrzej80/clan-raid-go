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
)

type boardCall struct {
	raidID uuid.UUID
	totals map[int64]int64
	hpLeft int64
}

// recordingBoard запоминает вызовы Apply и может вернуть ошибку.
type recordingBoard struct {
	mu    sync.Mutex
	calls []boardCall
	err   error
}

func (b *recordingBoard) Apply(_ context.Context, raidID uuid.UUID, totals []domain.PlayerDamage, hpLeft int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := boardCall{raidID: raidID, totals: map[int64]int64{}, hpLeft: hpLeft}
	for _, t := range totals {
		c.totals[t.PlayerID] = t.Damage
	}
	b.calls = append(b.calls, c)
	return b.err
}

func (b *recordingBoard) callsFor(raidID uuid.UUID) []boardCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []boardCall
	for _, c := range b.calls {
		if c.raidID == raidID {
			out = append(out, c)
		}
	}
	return out
}

// В Redis уходят абсолютные суммы из raid_damage (а не дельта пачки) и итоговое HP.
func TestHitsProcessor_BoardGetsAbsoluteTotals(t *testing.T) {
	t.Parallel()

	board := &recordingBoard{}
	env := newProcessorEnv(t, 2, raid.WithBoard(board))
	r := env.newRaidWithHP(t, 1000)

	mustProcess(t, env.processor, []domain.Hit{
		env.hit(r.ID, env.players[0], 100),
		env.hit(r.ID, env.players[1], 50),
	})
	mustProcess(t, env.processor, []domain.Hit{env.hit(r.ID, env.players[0], 30)})

	calls := board.callsFor(r.ID)
	if len(calls) != 2 {
		t.Fatalf("board calls = %d, want 2 (one per batch)", len(calls))
	}
	if calls[0].hpLeft != 850 || calls[0].totals[env.players[0]] != 100 || calls[0].totals[env.players[1]] != 50 {
		t.Fatalf("first call = %+v", calls[0])
	}
	// Во второй пачке только игрок 0, но его сумма накопленная: 100 + 30.
	if calls[1].hpLeft != 820 || calls[1].totals[env.players[0]] != 130 || len(calls[1].totals) != 1 {
		t.Fatalf("second call = %+v", calls[1])
	}
}

// Повторная пачка (после падения консьюмера) ничего нового не принимает, и Redis не трогается:
// её состояние уже записано первой обработкой, а при потере Redis выручает rebuild-cache.
func TestHitsProcessor_BoardNotCalledForDuplicatesAndLateHits(t *testing.T) {
	t.Parallel()

	board := &recordingBoard{}
	env := newProcessorEnv(t, 1, raid.WithBoard(board))
	r := env.newRaidWithHP(t, 100)
	batch := []domain.Hit{env.hit(r.ID, env.players[0], 100)} // убивает босса

	mustProcess(t, env.processor, batch)
	mustProcess(t, env.processor, batch)                                           // дубль
	mustProcess(t, env.processor, []domain.Hit{env.hit(r.ID, env.players[0], 10)}) // рейд уже defeated

	calls := board.callsFor(r.ID)
	if len(calls) != 1 || calls[0].hpLeft != 0 {
		t.Fatalf("board calls = %+v, want exactly one with hp 0", calls)
	}
}

// Падение Redis не откатывает и не повторяет пачку: Postgres уже зафиксирован.
func TestHitsProcessor_BoardFailureDoesNotFailBatch(t *testing.T) {
	t.Parallel()

	board := &recordingBoard{err: errors.New("redis down")}
	env := newProcessorEnv(t, 1, raid.WithBoard(board))
	r := env.newRaidWithHP(t, 1000)

	res, err := env.processor.ProcessBatch(context.Background(), []domain.Hit{env.hit(r.ID, env.players[0], 100)})
	if err != nil || len(res.Rejected) != 0 {
		t.Fatalf("ProcessBatch: err = %v, rejected = %+v, want success", err, res.Rejected)
	}
	if n := env.count(t, `SELECT total_damage FROM raid_damage WHERE raid_id = $1`, r.ID); n != 100 {
		t.Fatalf("total_damage = %d, want 100: postgres must stay committed", n)
	}
}

func TestDamageRepo_TopDamage(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 3)
	r := env.newRaidWithHP(t, 10_000)
	mustProcess(t, env.processor, []domain.Hit{
		env.hit(r.ID, env.players[0], 100),
		env.hit(r.ID, env.players[1], 300),
		env.hit(r.ID, env.players[2], 200),
	})

	repo := pg.NewDamageRepo(env.pool)
	got, err := repo.TopDamage(context.Background(), r.ID, 2)
	if err != nil {
		t.Fatalf("TopDamage: %v", err)
	}
	want := []domain.LeaderboardEntry{
		{PlayerID: env.players[1], TotalDamage: 300},
		{PlayerID: env.players[2], TotalDamage: 200},
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("top = %+v, want %+v", got, want)
	}

	empty, err := repo.TopDamage(context.Background(), uuid.New(), 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("top of unknown raid = %+v, err = %v, want empty", empty, err)
	}
}
