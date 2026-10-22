// Package raid содержит бизнес-логику рейдов. Пакет не знает ни про gRPC, ни про protobuf.
package raid

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type Service struct {
	raids domain.RaidRepository
	now   func() time.Time
	newID func() uuid.UUID
}

func NewService(raids domain.RaidRepository) *Service {
	return &Service{raids: raids, now: time.Now, newID: uuid.New}
}

type CreateParams struct {
	ClanID   int64
	BossName string
	HPMax    int64
	Duration time.Duration
}

func (p CreateParams) validate() error {
	switch {
	case p.ClanID <= 0:
		return fmt.Errorf("%w: clan_id must be positive", domain.ErrInvalidArgument)
	case p.BossName == "":
		return fmt.Errorf("%w: boss_name is required", domain.ErrInvalidArgument)
	case p.HPMax <= 0:
		return fmt.Errorf("%w: hp_max must be positive", domain.ErrInvalidArgument)
	case p.Duration <= 0:
		return fmt.Errorf("%w: duration must be positive", domain.ErrInvalidArgument)
	}
	return nil
}

// Create заводит активный рейд для клана. Что у клана не больше одного активного рейда,
// гарантирует БД (ErrActiveRaidExists).
func (s *Service) Create(ctx context.Context, p CreateParams) (*domain.Raid, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	now := s.now().UTC()
	raid := &domain.Raid{
		ID:        s.newID(),
		ClanID:    p.ClanID,
		BossName:  p.BossName,
		HPMax:     p.HPMax,
		HPLeft:    p.HPMax,
		Status:    domain.RaidActive,
		StartedAt: now,
		EndsAt:    now.Add(p.Duration),
	}
	if err := s.raids.CreateRaid(ctx, raid); err != nil {
		return nil, err
	}
	return raid, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (*domain.Raid, error) {
	return s.raids.GetRaid(ctx, id)
}
