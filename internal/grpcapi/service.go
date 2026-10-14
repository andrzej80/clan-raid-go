package grpcapi

import (
	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type RaidService struct {
	raidbossV1.UnimplementedRaidServiceServer
	raids domain.RaidRepository
	tx    domain.TxManager
}

func NewRaidService(raids domain.RaidRepository, tx domain.TxManager) *RaidService {
	return &RaidService{raids: raids, tx: tx}
}
