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
	"github.com/andrzej80/clan-raid-go/internal/raid"
)

type stubLeaderboard struct {
	entries []domain.LeaderboardEntry
	err     error
	limit   int
}

func (s *stubLeaderboard) Top(_ context.Context, _ uuid.UUID, limit int) ([]domain.LeaderboardEntry, error) {
	s.limit = limit
	return s.entries, s.err
}

func newLeaderboardClient(t *testing.T, board *stubLeaderboard) raidbossV1.RaidServiceClient {
	t.Helper()

	conn := startTestServerWith(t, &config.Config{}, grpcapi.NewRaidService(grpcapi.WithLeaderboard(grpcapi.NewLeaderboardHandlers(board))))
	return raidbossV1.NewRaidServiceClient(conn)
}

func TestAPI_GetLeaderboard(t *testing.T) {
	t.Parallel()
	board := &stubLeaderboard{entries: []domain.LeaderboardEntry{
		{PlayerID: 2, TotalDamage: 300}, {PlayerID: 1, TotalDamage: 100},
	}}
	client := newLeaderboardClient(t, board)

	resp, err := client.GetLeaderboard(testCtx(t), &raidbossV1.GetLeaderboardRequest{
		RaidId: uuid.NewString(), Limit: 5,
	})
	if err != nil {
		t.Fatalf("GetLeaderboard: %v", err)
	}
	entries := resp.GetEntries()
	if len(entries) != 2 || entries[0].GetPlayerId() != 2 || entries[0].GetTotalDamage() != 300 ||
		entries[1].GetPlayerId() != 1 {
		t.Fatalf("entries = %v", entries)
	}
	if board.limit != 5 {
		t.Fatalf("limit passed to service = %d, want 5", board.limit)
	}
}

func TestAPI_GetLeaderboardErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		raidID string
		err    error
		want   codes.Code
	}{
		{"invalid uuid", "not-a-uuid", nil, codes.InvalidArgument},
		{"unknown raid", uuid.NewString(), domain.ErrRaidNotFound, codes.NotFound},
		{"invalid limit", uuid.NewString(), domain.ErrInvalidArgument, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newLeaderboardClient(t, &stubLeaderboard{err: tt.err})

			_, err := client.GetLeaderboard(testCtx(t), &raidbossV1.GetLeaderboardRequest{RaidId: tt.raidID})
			requireCode(t, err, tt.want)
		})
	}
}

func TestAPI_GetLeaderboardRequiresPlayerID(t *testing.T) {
	t.Parallel()
	client := newLeaderboardClient(t, &stubLeaderboard{})

	_, err := client.GetLeaderboard(context.Background(), &raidbossV1.GetLeaderboardRequest{RaidId: uuid.NewString()})
	requireCode(t, err, codes.Unauthenticated)
}

// Рейд, которого нет, и отрицательный limit доходят до настоящего сервиса и дают правильные коды.
func TestAPI_GetLeaderboardRealServiceValidation(t *testing.T) {
	t.Parallel()
	svc, err := raid.NewLeaderboardService(&fakeRaids{}, nil, nil)
	if err != nil {
		t.Fatalf("NewLeaderboardService: %v", err)
	}
	conn := startTestServerWith(t, &config.Config{}, grpcapi.NewRaidService(grpcapi.WithLeaderboard(grpcapi.NewLeaderboardHandlers(svc))))
	client := raidbossV1.NewRaidServiceClient(conn)

	_, err = client.GetLeaderboard(testCtx(t), &raidbossV1.GetLeaderboardRequest{RaidId: uuid.NewString(), Limit: -1})
	requireCode(t, err, codes.InvalidArgument)

	_, err = client.GetLeaderboard(testCtx(t), &raidbossV1.GetLeaderboardRequest{RaidId: uuid.NewString()})
	requireCode(t, err, codes.NotFound)
}
