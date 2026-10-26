// Package scheduler переводит просроченные рейды в expired (раздел 6.5 README).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const meterName = "github.com/andrzej80/clan-raid-go/internal/scheduler"

const eventTypeRaidExpired = "RaidExpired"

type Config struct {
	// Interval — как часто искать просроченные рейды.
	Interval time.Duration
	// EventsTopic — топик событий рейдов; имя пишется в outbox.topic.
	EventsTopic string
	// MeterProvider необязателен: по умолчанию используется глобальный.
	MeterProvider metric.MeterProvider
	// Publisher необязателен: без него подписчики WatchRaid узнают об истечении только после переподключения.
	Publisher ExpiredPublisher
}

// ExpiredPublisher сообщает подписчикам WatchRaid, что время рейда вышло.
type ExpiredPublisher interface {
	PublishExpired(ctx context.Context, raidID uuid.UUID) error
}

// publishTimeout ограничивает уведомление подписчиков: оно не должно тормозить проход.
const publishTimeout = 500 * time.Millisecond

// Scheduler раз в Interval переводит просроченные активные рейды в expired и в той же транзакции
// пишет RaidEvent{expired} в outbox. Безопасен в нескольких экземплярах: UPDATE ... WHERE
// status = 'active' отдаёт каждый рейд только одному из них.
type Scheduler struct {
	tx      domain.TxManager
	expiry  domain.ExpiryRepository
	outbox  domain.OutboxRepository
	cfg     Config
	metrics *schedulerMetrics
}

func New(
	tx domain.TxManager, expiry domain.ExpiryRepository, outbox domain.OutboxRepository, cfg Config,
) (*Scheduler, error) {
	if cfg.Interval <= 0 || cfg.EventsTopic == "" {
		return nil, errors.New("scheduler: interval must be positive and events topic is required")
	}
	metrics, err := newSchedulerMetrics(cfg.MeterProvider)
	if err != nil {
		return nil, err
	}
	return &Scheduler{tx: tx, expiry: expiry, outbox: outbox, cfg: cfg, metrics: metrics}, nil
}

// Run выполняет проход сразу и затем каждые Interval до отмены ctx. Ошибка прохода не останавливает
// цикл: следующий проход повторит работу, а откат транзакции не оставляет полупереведённых рейдов.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		if n, err := s.ExpireOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("expire raids", "err", err)
			s.metrics.errors.Add(ctx, 1)
		} else if n > 0 {
			slog.Info("raids expired", "count", n)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

// ExpireOnce выполняет один проход и возвращает число истёкших рейдов.
func (s *Scheduler) ExpireOnce(ctx context.Context) (int, error) {
	var expired []domain.ExpiredRaid
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		expired = nil // функция может выполниться повторно при ретрае транзакции

		var err error
		if expired, err = s.expiry.ExpireDue(ctx); err != nil {
			return err
		}
		for _, raid := range expired {
			if err := s.addExpiredEvent(ctx, raid); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	s.metrics.expired.Add(ctx, int64(len(expired)))
	s.publishExpired(ctx, expired)
	return len(expired), nil
}

// publishExpired уведомляет подписчиков после коммита; ошибка не откатывает проход (best-effort,
// клиент получит итог в snapshot при переподключении).
func (s *Scheduler) publishExpired(ctx context.Context, expired []domain.ExpiredRaid) {
	if s.cfg.Publisher == nil {
		return
	}
	for _, raid := range expired {
		pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
		err := s.cfg.Publisher.PublishExpired(pubCtx, raid.ID)
		cancel()
		if err != nil {
			slog.Warn("publish raid expired failed", "raid_id", raid.ID, "err", err)
			s.metrics.errors.Add(ctx, 1)
		}
	}
}

func (s *Scheduler) addExpiredEvent(ctx context.Context, raid domain.ExpiredRaid) error {
	payload, err := proto.Marshal(&eventsV1.RaidEvent{
		RaidId:     raid.ID.String(),
		OccurredAt: timestamppb.New(raid.FinishedAt),
		Event:      &eventsV1.RaidEvent_Expired{Expired: &eventsV1.RaidExpired{HpLeft: raid.HPLeft}},
	})
	if err != nil {
		return fmt.Errorf("marshal raid expired event: %w", err)
	}
	return s.outbox.Add(ctx, domain.OutboxMessage{
		Topic:   s.cfg.EventsTopic,
		Key:     raid.ID.String(),
		Payload: payload,
		Headers: map[string]string{"type": eventTypeRaidExpired},
	})
}

type schedulerMetrics struct {
	expired metric.Int64Counter
	errors  metric.Int64Counter
}

func newSchedulerMetrics(mp metric.MeterProvider) (*schedulerMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	expired, err := meter.Int64Counter("raidboss.scheduler.expired",
		metric.WithDescription("Raids moved to expired"),
		metric.WithUnit("{raid}"))
	if err != nil {
		return nil, fmt.Errorf("create expired counter: %w", err)
	}
	errs, err := meter.Int64Counter("raidboss.scheduler.errors",
		metric.WithDescription("Failed expiry passes (rolled back, retried on the next tick)"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("create errors counter: %w", err)
	}
	return &schedulerMetrics{expired: expired, errors: errs}, nil
}
