package grpcapi

import (
	"context"

	"github.com/google/uuid"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/raid"
)

// raidService — то, что транспорту нужно от бизнес-логики рейдов.
type raidService interface {
	Create(ctx context.Context, p raid.CreateParams) (*domain.Raid, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.Raid, error)
}

// RaidService — gRPC-адаптер: переводит proto в вызовы сервиса и обратно, бизнес-логики здесь нет.
type RaidService struct {
	raidbossV1.UnimplementedRaidServiceServer
	raids raidService
}

func NewRaidService(raids raidService) *RaidService {
	return &RaidService{raids: raids}
}
