package grpcapi

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	raidsvc "github.com/andrzej80/clan-raid-go/internal/raid"
)

func (s *RaidService) CreateRaid(
	ctx context.Context,
	req *raidbossV1.CreateRaidRequest,
) (*raidbossV1.CreateRaidResponse, error) {
	raid, err := s.raids.Create(ctx, raidsvc.CreateParams{
		ClanID:   req.GetClanId(),
		BossName: req.GetBossName(),
		HPMax:    req.GetHpMax(),
		Duration: time.Duration(req.GetDurationSec()) * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &raidbossV1.CreateRaidResponse{Raid: raidToProto(raid)}, nil
}

func (s *RaidService) GetRaid(
	ctx context.Context,
	req *raidbossV1.GetRaidRequest,
) (*raidbossV1.GetRaidResponse, error) {
	id, err := uuid.Parse(req.GetRaidId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "raid_id must be a valid UUID")
	}

	raid, err := s.raids.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &raidbossV1.GetRaidResponse{Raid: raidToProto(raid)}, nil
}

func raidToProto(r *domain.Raid) *raidbossV1.Raid {
	return &raidbossV1.Raid{
		Id:       r.ID.String(),
		ClanId:   r.ClanID,
		BossName: r.BossName,
		HpMax:    r.HPMax,
		HpLeft:   r.HPLeft,
		Status:   statusToProto(r.Status),
		EndsAt:   timestamppb.New(r.EndsAt),
	}
}

func statusToProto(s domain.RaidStatus) raidbossV1.RaidStatus {
	switch s {
	case domain.RaidActive:
		return raidbossV1.RaidStatus_RAID_STATUS_ACTIVE
	case domain.RaidDefeated:
		return raidbossV1.RaidStatus_RAID_STATUS_DEFEATED
	case domain.RaidExpired:
		return raidbossV1.RaidStatus_RAID_STATUS_EXPIRED
	default:
		return raidbossV1.RaidStatus_RAID_STATUS_UNSPECIFIED
	}
}

func (s *RaidService) Attack(
	ctx context.Context,
	req *raidbossV1.AttackRequest,
) (*raidbossV1.AttackResponse, error) {
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

	res, err := s.attacks.Attack(ctx, raidsvc.AttackParams{
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

func (s *RaidService) GetLeaderboard(
	ctx context.Context,
	req *raidbossV1.GetLeaderboardRequest,
) (*raidbossV1.GetLeaderboardResponse, error) {
	id, err := uuid.Parse(req.GetRaidId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "raid_id must be a valid UUID")
	}

	entries, err := s.board.Top(ctx, id, int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := make([]*raidbossV1.LeaderboardEntry, len(entries))
	for i, e := range entries {
		out[i] = &raidbossV1.LeaderboardEntry{PlayerId: e.PlayerID, TotalDamage: e.TotalDamage}
	}
	return &raidbossV1.GetLeaderboardResponse{Entries: out}, nil
}

// ListRewards отдаёт награды игрока из x-player-id. Поле player_id запроса устарело и игнорируется:
// чужие награды по подставленному id получить нельзя.
func (s *RaidService) ListRewards(
	ctx context.Context,
	_ *raidbossV1.ListRewardsRequest,
) (*raidbossV1.ListRewardsResponse, error) {
	playerID, ok := interceptors.PlayerIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, interceptors.PlayerIDHeader+" metadata is required")
	}

	rewards, err := s.rewards.List(ctx, playerID)
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
