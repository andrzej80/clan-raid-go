package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
)

type stubRewards struct {
	rewards  []domain.PlayerReward
	err      error
	playerID int64
}

func (s *stubRewards) List(_ context.Context, playerID int64) ([]domain.PlayerReward, error) {
	s.playerID = playerID
	return s.rewards, s.err
}

func newRewardsClient(t *testing.T, rewards *stubRewards) raidbossV1.RaidServiceClient {
	t.Helper()

	conn := startTestServerWith(t, &config.Config{}, grpcapi.NewRaidService(grpcapi.WithRewards(grpcapi.NewRewardsHandlers(rewards))))
	return raidbossV1.NewRaidServiceClient(conn)
}

func TestAPI_ListRewards(t *testing.T) {
	t.Parallel()
	raidID := uuid.New()
	stub := &stubRewards{rewards: []domain.PlayerReward{
		{RaidID: raidID, Kind: domain.TopKind, Amount: 500},
		{RaidID: raidID, Kind: domain.GeneralKind, Amount: 850},
	}}
	client := newRewardsClient(t, stub)

	resp, err := client.ListRewards(playerCtx(t, 42), &raidbossV1.ListRewardsRequest{})
	if err != nil {
		t.Fatalf("ListRewards: %v", err)
	}
	got := resp.GetRewards()
	if len(got) != 2 || got[0].GetRaidId() != raidID.String() || got[0].GetKind() != "top3" ||
		got[0].GetAmount() != 500 || got[1].GetKind() != "participation" || got[1].GetAmount() != 850 {
		t.Fatalf("rewards = %v", got)
	}
	if stub.playerID != 42 {
		t.Fatalf("service got player %d, want 42 from x-player-id", stub.playerID)
	}
}

// Поле player_id устарело: чужие награды по подставленному id получить нельзя.
func TestAPI_ListRewardsIgnoresRequestPlayerID(t *testing.T) {
	t.Parallel()
	stub := &stubRewards{}
	client := newRewardsClient(t, stub)

	//nolint:staticcheck // проверяем, что устаревшее поле не влияет на результат
	_, err := client.ListRewards(playerCtx(t, 42), &raidbossV1.ListRewardsRequest{PlayerId: 99})
	if err != nil {
		t.Fatalf("ListRewards: %v", err)
	}
	if stub.playerID != 42 {
		t.Fatalf("service got player %d, want 42: request player_id must be ignored", stub.playerID)
	}
}

func TestAPI_ListRewardsEmpty(t *testing.T) {
	t.Parallel()
	client := newRewardsClient(t, &stubRewards{})

	resp, err := client.ListRewards(playerCtx(t, 1), &raidbossV1.ListRewardsRequest{})
	if err != nil || len(resp.GetRewards()) != 0 {
		t.Fatalf("ListRewards = %v, %v; want empty list", resp, err)
	}
}

func TestAPI_ListRewardsErrors(t *testing.T) {
	t.Parallel()

	t.Run("unknown player", func(t *testing.T) {
		t.Parallel()
		client := newRewardsClient(t, &stubRewards{err: domain.ErrPlayerNotFound})

		_, err := client.ListRewards(playerCtx(t, 1), &raidbossV1.ListRewardsRequest{})
		requireCode(t, err, codes.NotFound)
	})
	t.Run("no x-player-id", func(t *testing.T) {
		t.Parallel()
		client := newRewardsClient(t, &stubRewards{})

		_, err := client.ListRewards(context.Background(), &raidbossV1.ListRewardsRequest{})
		requireCode(t, err, codes.Unauthenticated)
	})
}
