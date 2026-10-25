package raid

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type stubPlayerReader struct{ known int64 }

func (s stubPlayerReader) GetPlayer(_ context.Context, id int64) (*domain.Player, error) {
	if id != s.known {
		return nil, domain.ErrPlayerNotFound
	}
	return &domain.Player{ID: id}, nil
}

type stubRewardsReader struct {
	rewards []domain.PlayerReward
	limit   int
	called  bool
}

func (s *stubRewardsReader) ListByPlayer(_ context.Context, _ int64, limit int) ([]domain.PlayerReward, error) {
	s.called, s.limit = true, limit
	return s.rewards, nil
}

func TestRewardsService_List(t *testing.T) {
	t.Parallel()

	want := []domain.PlayerReward{{RaidID: uuid.New(), Kind: domain.TopKind, Amount: 500}}
	reader := &stubRewardsReader{rewards: want}
	svc := NewRewardsService(stubPlayerReader{known: 7}, reader)

	got, err := svc.List(context.Background(), 7)
	if err != nil || len(got) != 1 || got[0] != want[0] {
		t.Fatalf("List = %v, %v; want %v", got, err, want)
	}
	if reader.limit != maxRewardsListed {
		t.Fatalf("limit = %d, want %d", reader.limit, maxRewardsListed)
	}
}

func TestRewardsService_UnknownPlayer(t *testing.T) {
	t.Parallel()

	reader := &stubRewardsReader{}
	svc := NewRewardsService(stubPlayerReader{known: 7}, reader)

	_, err := svc.List(context.Background(), 8)
	if !errors.Is(err, domain.ErrPlayerNotFound) {
		t.Fatalf("List error = %v, want ErrPlayerNotFound", err)
	}
	if reader.called {
		t.Fatal("rewards must not be read for an unknown player")
	}
}
