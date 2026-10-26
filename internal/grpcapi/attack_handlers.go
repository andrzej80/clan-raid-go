package grpcapi

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	raidsvc "github.com/andrzej80/clan-raid-go/internal/raid"
)

// attackService принимает удары.
type attackService interface {
	Attack(ctx context.Context, p raidsvc.AttackParams) (raidsvc.AttackResult, error)
}

// AttackHandlers — Attack.
type AttackHandlers struct {
	attacks attackService
}

func NewAttackHandlers(attacks attackService) *AttackHandlers {
	return &AttackHandlers{attacks: attacks}
}

func (h *AttackHandlers) Attack(
	ctx context.Context,
	req *raidbossV1.AttackRequest,
) (*raidbossV1.AttackResponse, error) {
	if h == nil {
		return nil, errNotEnabled
	}
	raidID, err := uuid.Parse(req.GetRaidId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "raid_id must be a valid UUID")
	}
	key, err := uuid.Parse(req.GetIdempotencyKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key must be a valid UUID")
	}
	playerID, ok := interceptors.PlayerIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, interceptors.PlayerIDHeader+" metadata is required")
	}

	res, err := h.attacks.Attack(ctx, raidsvc.AttackParams{
		RaidID:         raidID,
		PlayerID:       playerID,
		IdempotencyKey: key,
	})
	if err != nil {
		return nil, err
	}
	return &raidbossV1.AttackResponse{
		HitId:  res.HitID.String(),
		Damage: int32(res.Damage), //nolint:gosec // урон ограничен силой игрока (≤ 1000 × 1.2 × 2)
		IsCrit: res.IsCrit,
	}, nil
}
