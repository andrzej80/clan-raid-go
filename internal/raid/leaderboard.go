package raid

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/andrzej80/clan-raid-go/internal/cache"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const (
	defaultLeaderboardLimit = 10
	maxLeaderboardLimit     = 100

	// Раз в секунду на пару (рейд, limit) — не чаще одного запроса к Postgres при любой нагрузке на фоллбэк.
	leaderboardFallbackTTL = time.Second
	leaderboardRaidTTL     = time.Second
	// Redis отвечает за доли миллисекунды: если нет, лучше быстро уйти в Postgres, чем ждать общий таймаут клиента.
	defaultBoardTimeout = 200 * time.Millisecond

	// Значения атрибута reason у счётчика фоллбэка.
	fallbackReasonError = "error"
	fallbackReasonEmpty = "empty"
)

// BoardReader — лидерборд в быстром хранилище (Redis).
type BoardReader interface {
	Top(ctx context.Context, raidID uuid.UUID, limit int) ([]domain.LeaderboardEntry, error)
}

type topKey struct {
	raidID uuid.UUID
	limit  int
}

// LeaderboardService читает лидерборд из Redis, а при его сбое или потере данных — из Postgres.
type LeaderboardService struct {
	raids        *cache.TTL[uuid.UUID, *domain.Raid]
	board        BoardReader
	fallback     *cache.TTL[topKey, []domain.LeaderboardEntry]
	boardTimeout time.Duration
	fallbacks    metric.Int64Counter
}

type LeaderboardOption func(*leaderboardOptions)

type leaderboardOptions struct {
	now          func() time.Time
	boardTimeout time.Duration
	meters       metric.MeterProvider
}

// WithLeaderboardClock подменяет источник времени кэшей (для тестов).
func WithLeaderboardClock(now func() time.Time) LeaderboardOption {
	return func(o *leaderboardOptions) { o.now = now }
}

// WithBoardTimeout ограничивает одно чтение из Redis.
func WithBoardTimeout(d time.Duration) LeaderboardOption {
	return func(o *leaderboardOptions) { o.boardTimeout = d }
}

// WithLeaderboardMeterProvider подменяет провайдер метрик (для тестов).
func WithLeaderboardMeterProvider(mp metric.MeterProvider) LeaderboardOption {
	return func(o *leaderboardOptions) { o.meters = mp }
}

func NewLeaderboardService(
	raids domain.RaidRepository,
	board BoardReader,
	damage domain.DamageReader,
	opts ...LeaderboardOption,
) (*LeaderboardService, error) {
	o := leaderboardOptions{now: time.Now, boardTimeout: defaultBoardTimeout, meters: otel.GetMeterProvider()}
	for _, opt := range opts {
		opt(&o)
	}

	fallbacks, err := o.meters.Meter(processorMeterName).Int64Counter("raidboss.leaderboard.fallback",
		metric.WithDescription("Leaderboard reads served from PostgreSQL instead of Redis, by reason"),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("create leaderboard fallback counter: %w", err)
	}

	return &LeaderboardService{
		raids: cache.NewTTL(leaderboardRaidTTL, func(ctx context.Context, id uuid.UUID) (*domain.Raid, error) {
			return raids.GetRaid(ctx, id)
		}, cache.WithClock(o.now)),
		board: board,
		fallback: cache.NewTTL(leaderboardFallbackTTL, func(ctx context.Context, k topKey) ([]domain.LeaderboardEntry, error) {
			return damage.TopDamage(ctx, k.raidID, k.limit)
		}, cache.WithClock(o.now)),
		boardTimeout: o.boardTimeout,
		fallbacks:    fallbacks,
	}, nil
}

// Top возвращает до limit игроков рейда по убыванию урона; limit == 0 означает значение по умолчанию.
func (s *LeaderboardService) Top(ctx context.Context, raidID uuid.UUID, limit int) ([]domain.LeaderboardEntry, error) {
	switch {
	case limit < 0:
		return nil, fmt.Errorf("%w: limit must not be negative", domain.ErrInvalidArgument)
	case limit == 0:
		limit = defaultLeaderboardLimit
	case limit > maxLeaderboardLimit:
		limit = maxLeaderboardLimit
	}

	raid, err := s.raids.Get(ctx, raidID)
	if err != nil {
		return nil, err
	}

	boardCtx, cancel := context.WithTimeout(ctx, s.boardTimeout)
	entries, err := s.board.Top(boardCtx, raidID, limit)
	cancel()
	switch {
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		slog.Warn("leaderboard: redis read failed, falling back to postgres", "raid_id", raidID, "err", err)
		s.countFallback(ctx, fallbackReasonError)
	case len(entries) == 0 && raid.HPLeft < raid.HPMax:
		// Урон уже нанесён, а лидерборд пуст: Redis потерял данные (сброс, рестарт без persistence).
		s.countFallback(ctx, fallbackReasonEmpty)
	default:
		return entries, nil
	}

	// Кэш с singleflight: сколько бы клиентов ни пришло, в Postgres уходит не больше запроса в секунду
	// на пару (рейд, limit).
	return s.fallback.Get(ctx, topKey{raidID: raidID, limit: limit})
}

func (s *LeaderboardService) countFallback(ctx context.Context, reason string) {
	s.fallbacks.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
