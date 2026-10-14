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
)

func (s *RaidService) CreateRaid(
	ctx context.Context,
	req *raidbossV1.CreateRaidRequest,
) (*raidbossV1.CreateRaidResponse, error) {
	if req.GetClanId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "clan_id must be positive")
	}
	if req.GetBossName() == "" {
		return nil, status.Error(codes.InvalidArgument, "boss_name is required")
	}
	if req.GetHpMax() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "hp_max must be positive")
	}
	if req.GetDurationSec() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "duration_sec must be positive")
	}

	now := time.Now().UTC()
	raid := &domain.Raid{
		ID:        uuid.New(),
		ClanID:    req.GetClanId(),
		BossName:  req.GetBossName(),
		HPMax:     req.GetHpMax(),
		HPLeft:    req.GetHpMax(),
		Status:    domain.RaidActive,
		StartedAt: now,
		EndsAt:    now.Add(time.Duration(req.GetDurationSec()) * time.Second),
	}

	if err := s.raids.CreateRaid(ctx, raid); err != nil {
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

	raid, err := s.raids.GetRaid(ctx, id)
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
