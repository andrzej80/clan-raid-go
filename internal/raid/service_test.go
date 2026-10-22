package raid

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type stubRepo struct {
	created []*domain.Raid
	err     error
}

func (r *stubRepo) CreateRaid(_ context.Context, raid *domain.Raid) error {
	if r.err != nil {
		return r.err
	}
	r.created = append(r.created, raid)
	return nil
}

func (r *stubRepo) GetRaid(context.Context, uuid.UUID) (*domain.Raid, error) {
	return nil, domain.ErrRaidNotFound
}

func validParams() CreateParams {
	return CreateParams{ClanID: 1, BossName: "Dragon", HPMax: 1000, Duration: time.Hour}
}

func TestService_Create(t *testing.T) {
	t.Parallel()

	repo := &stubRepo{}
	svc := NewService(repo)
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	id := uuid.New()
	svc.now = func() time.Time { return now }
	svc.newID = func() uuid.UUID { return id }

	raid, err := svc.Create(context.Background(), validParams())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if raid.ID != id || raid.HPLeft != raid.HPMax || raid.Status != domain.RaidActive ||
		!raid.StartedAt.Equal(now) || !raid.EndsAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("unexpected raid: %+v", raid)
	}
	if len(repo.created) != 1 || repo.created[0] != raid {
		t.Fatalf("raid must be passed to repository exactly once")
	}
}

func TestService_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*CreateParams)
	}{
		{"no clan", func(p *CreateParams) { p.ClanID = 0 }},
		{"negative clan", func(p *CreateParams) { p.ClanID = -5 }},
		{"no boss name", func(p *CreateParams) { p.BossName = "" }},
		{"zero hp", func(p *CreateParams) { p.HPMax = 0 }},
		{"zero duration", func(p *CreateParams) { p.Duration = 0 }},
		{"negative duration", func(p *CreateParams) { p.Duration = -time.Second }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := &stubRepo{}
			p := validParams()
			tc.mutate(&p)

			_, err := NewService(repo).Create(context.Background(), p)
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
			if len(repo.created) != 0 {
				t.Fatal("invalid raid must not reach the repository")
			}
		})
	}
}

func TestService_CreateRepoErrorPassedThrough(t *testing.T) {
	t.Parallel()

	svc := NewService(&stubRepo{err: domain.ErrActiveRaidExists})

	_, err := svc.Create(context.Background(), validParams())
	if !errors.Is(err, domain.ErrActiveRaidExists) {
		t.Fatalf("err = %v, want ErrActiveRaidExists", err)
	}
}
