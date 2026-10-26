package grpcapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
)

// rewardsService отдаёт награды игрока.
type rewardsService interface {
	List(ctx context.Context, playerID int64) ([]domain.PlayerReward, error)
}

// RewardsHandlers — ListRewards.
type RewardsHandlers struct {
	rewards rewardsService
}

func NewRewardsHandlers(rewards rewardsService) *RewardsHandlers {
	return &RewardsHandlers{rewards: rewards}
}

// ListRewards отдаёт награды игрока из x-player-id. Поле player_id запроса устарело и игнорируется:
// чужие награды по подставленному id получить нельзя.
func (h *RewardsHandlers) ListRewards(
	ctx context.Context,
	_ *raidbossV1.ListRewardsRequest,
) (*raidbossV1.ListRewardsResponse, error) {
	if h == nil {
		return nil, errNotEnabled
	}
	playerID, ok := interceptors.PlayerIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, interceptors.PlayerIDHeader+" metadata is required")
	}

	rewards, err := h.rewards.List(ctx, playerID)
	if err != nil {
		return nil, err
	}
	out := make([]*raidbossV1.Reward, len(rewards))
	for i, r := range rewards {
		out[i] = &raidbossV1.Reward{
			RaidId: r.RaidID.String(),
			Kind:   string(r.Kind),
			Amount: int32(r.Amount), //nolint:gosec // награда ограничена правилами расчёта (< 2000)
		}
	}
	return &raidbossV1.ListRewardsResponse{Rewards: out}, nil
}
