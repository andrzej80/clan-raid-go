package grpcapi

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// leaderboardService отдаёт лидерборд рейда.
type leaderboardService interface {
	Top(ctx context.Context, raidID uuid.UUID, limit int) ([]domain.LeaderboardEntry, error)
}

// LeaderboardHandlers — GetLeaderboard.
type LeaderboardHandlers struct {
	board leaderboardService
}

func NewLeaderboardHandlers(board leaderboardService) *LeaderboardHandlers {
	return &LeaderboardHandlers{board: board}
}

func (h *LeaderboardHandlers) GetLeaderboard(
	ctx context.Context,
	req *raidbossV1.GetLeaderboardRequest,
) (*raidbossV1.GetLeaderboardResponse, error) {
	if h == nil {
		return nil, errNotEnabled
	}
	id, err := uuid.Parse(req.GetRaidId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "raid_id must be a valid UUID")
	}

	entries, err := h.board.Top(ctx, id, int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return leaderboardToProto(entries), nil
}

func leaderboardToProto(entries []domain.LeaderboardEntry) *raidbossV1.GetLeaderboardResponse {
	out := make([]*raidbossV1.LeaderboardEntry, len(entries))
	for i, e := range entries {
		out[i] = &raidbossV1.LeaderboardEntry{PlayerId: e.PlayerID, TotalDamage: e.TotalDamage}
	}
	return &raidbossV1.GetLeaderboardResponse{Entries: out}
}
