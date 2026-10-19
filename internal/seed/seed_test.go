package seed

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type fakeCatalog struct {
	clans   []string
	players []domain.Player
	err     error
}

func (f *fakeCatalog) CreateClans(_ context.Context, names []string) ([]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	ids := make([]int64, len(names))
	for i := range names {
		ids[i] = int64(len(f.clans) + i + 1)
	}
	f.clans = append(f.clans, names...)
	return ids, nil
}

func (f *fakeCatalog) CreatePlayers(_ context.Context, players []domain.Player) error {
	f.players = append(f.players, players...)
	return nil
}

type passTx struct{}

func (passTx) WithinTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

func newRng() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func TestRun(t *testing.T) {
	t.Parallel()

	repo := &fakeCatalog{}
	res, err := Run(context.Background(), repo, passTx{}, Options{Clans: 100, PlayersPerClan: 50}, newRng())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Clans != 100 || res.Players != 5000 {
		t.Fatalf("result = %+v, want 100 clans / 5000 players", res)
	}
	if len(repo.clans) != 100 || len(repo.players) != 5000 {
		t.Fatalf("stored %d clans / %d players", len(repo.clans), len(repo.players))
	}

	perClan := map[int64]int{}
	for _, p := range repo.players {
		perClan[p.ClanID]++
		if p.Power < domain.MinPlayerPower || p.Power > domain.MaxPlayerPower {
			t.Fatalf("power %d out of range", p.Power)
		}
	}
	if len(perClan) != 100 {
		t.Fatalf("players spread over %d clans, want 100", len(perClan))
	}
	for clan, n := range perClan {
		if n != 50 {
			t.Fatalf("clan %d has %d players, want 50", clan, n)
		}
	}
}

func TestRun_InvalidOptions(t *testing.T) {
	t.Parallel()

	for _, opts := range []Options{{0, 10}, {10, 0}, {-1, 10}} {
		_, err := Run(context.Background(), &fakeCatalog{}, passTx{}, opts, newRng())
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("opts %+v: err = %v, want ErrInvalidArgument", opts, err)
		}
	}
}

func TestRun_RepoErrorPassedThrough(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	_, err := Run(context.Background(), &fakeCatalog{err: boom}, passTx{}, Options{Clans: 1, PlayersPerClan: 1}, newRng())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}
