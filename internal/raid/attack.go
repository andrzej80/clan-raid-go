package raid

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/cache"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const (
	playerCacheTTL = time.Minute
	raidCacheTTL   = time.Second
)

// HitPublisher отправляет принятый удар в очередь. Возврат nil означает, что брокер подтвердил запись.
type HitPublisher interface {
	Publish(ctx context.Context, hit domain.Hit) error
}

type AttackParams struct {
	RaidID         uuid.UUID
	PlayerID       int64
	IdempotencyKey uuid.UUID
}

type AttackResult struct {
	HitID  uuid.UUID
	Damage int
	IsCrit bool
}

// AttackService принимает удары. Он только проверяет запрос по кэшированным данным и кладёт удар
// в очередь: окончательное решение (засчитан или опоздал) принимает консьюмер.
type AttackService struct {
	players   *cache.TTL[int64, *domain.Player]
	raids     *cache.TTL[uuid.UUID, *domain.Raid]
	publisher HitPublisher
	now       func() time.Time
}

type AttackOption func(*attackOptions)

type attackOptions struct {
	now func() time.Time
}

// WithClock подменяет источник времени (для тестов).
func WithClock(now func() time.Time) AttackOption {
	return func(o *attackOptions) { o.now = now }
}

func NewAttackService(
	raids domain.RaidRepository,
	players domain.PlayerReader,
	publisher HitPublisher,
	opts ...AttackOption,
) *AttackService {
	o := attackOptions{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}

	return &AttackService{
		players: cache.NewTTL(playerCacheTTL, func(ctx context.Context, id int64) (*domain.Player, error) {
			return players.GetPlayer(ctx, id)
		}, cache.WithClock(o.now)),
		raids: cache.NewTTL(raidCacheTTL, func(ctx context.Context, id uuid.UUID) (*domain.Raid, error) {
			return raids.GetRaid(ctx, id)
		}, cache.WithClock(o.now)),
		publisher: publisher,
		now:       o.now,
	}
}

func (p AttackParams) validate() error {
	switch {
	case p.PlayerID <= 0:
		return fmt.Errorf("%w: player id must be positive", domain.ErrInvalidArgument)
	case p.RaidID == uuid.Nil:
		return fmt.Errorf("%w: raid_id is required", domain.ErrInvalidArgument)
	case p.IdempotencyKey == uuid.Nil:
		return fmt.Errorf("%w: idempotency_key is required", domain.ErrInvalidArgument)
	}
	return nil
}

// Attack проверяет, что игрок может бить этот рейд, считает урон и отправляет удар в очередь.
// Урон зависит только от силы игрока и idempotency_key, поэтому повтор запроса с тем же ключом
// возвращает тот же удар (а консьюмер отсекает дубль по hit_id).
func (s *AttackService) Attack(ctx context.Context, p AttackParams) (AttackResult, error) {
	if err := p.validate(); err != nil {
		return AttackResult{}, err
	}

	player, err := s.players.Get(ctx, p.PlayerID)
	if err != nil {
		return AttackResult{}, err
	}
	raid, err := s.raids.Get(ctx, p.RaidID)
	if err != nil {
		return AttackResult{}, err
	}

	now := s.now()
	if raid.ClanID != player.ClanID {
		return AttackResult{}, domain.ErrWrongClan
	}
	// Рейд из кэша может отставать до секунды, поэтому срок проверяем ещё и по ends_at.
	if raid.Status != domain.RaidActive || !now.Before(raid.EndsAt) {
		return AttackResult{}, domain.ErrRaidNotActive
	}

	damage, isCrit := domain.RollHitForKey(player.Power, p.IdempotencyKey)
	hit := domain.Hit{
		ID:        p.IdempotencyKey,
		RaidID:    raid.ID,
		PlayerID:  player.ID,
		ClanID:    player.ClanID,
		Damage:    damage,
		IsCrit:    isCrit,
		CreatedAt: now.UTC(),
	}
	if err := s.publisher.Publish(ctx, hit); err != nil {
		return AttackResult{}, publishError(err)
	}

	return AttackResult{HitID: hit.ID, Damage: damage, IsCrit: isCrit}, nil
}

// publishError: дедлайн и отмена клиента проходят как есть (это DEADLINE_EXCEEDED и CANCELED),
// любой другой сбой очереди — ErrQueueUnavailable, а подробности остаются в логе.
func publishError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	slog.Error("publish hit", "err", err)
	return domain.ErrQueueUnavailable
}
