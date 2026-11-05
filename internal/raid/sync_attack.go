package raid

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// HitApplier применяет один удар синхронно, в рамках запроса Attack.
// Возврат nil означает, что удар записан в Postgres (или это повтор уже записанного).
type HitApplier interface {
	ApplyHit(ctx context.Context, hit domain.Hit) error
}

// SyncPublisher подставляется в AttackService вместо очереди (ATTACK_MODE=sync и sync-full):
// удар применяется к Postgres до ответа клиенту.
type SyncPublisher struct {
	applier  HitApplier
	mode     string
	duration metric.Float64Histogram
}

// NewSyncPublisher создаёт публикатор; mode попадает в атрибут метрики длительности применения.
func NewSyncPublisher(applier HitApplier, mode string) (*SyncPublisher, error) {
	dur, err := otel.GetMeterProvider().Meter(guardMeterName).Float64Histogram("raidboss.attack.apply.duration",
		metric.WithDescription("Time to apply a hit synchronously in Attack (sync modes)"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create apply duration histogram: %w", err)
	}
	return &SyncPublisher{applier: applier, mode: mode, duration: dur}, nil
}

func (p *SyncPublisher) Publish(ctx context.Context, hit domain.Hit) error {
	start := time.Now()
	err := p.applier.ApplyHit(ctx, hit)
	p.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("mode", p.mode)))
	return err
}

// ProcessorApplier — вариант sync-full: тот же код, что в hits-consumer (HitsProcessor), но для пачки
// из одного удара и прямо в запросе. Запись raid_damage, событие в outbox при гибели босса,
// обновление Redis и уведомление подписчиков — всё как в режиме с очередью, поэтому проходят все
// инварианты `raidctl verify`. Разница с очередью только в том, что пачка всегда из одного удара.
type ProcessorApplier struct {
	Processor *HitsProcessor
}

func (a ProcessorApplier) ApplyHit(ctx context.Context, hit domain.Hit) error {
	res, err := a.Processor.ProcessBatch(ctx, []domain.Hit{hit})
	if err != nil {
		return err
	}
	if len(res.Rejected) > 0 {
		return fmt.Errorf("%w: %s", domain.ErrPlayerNotFound, res.Rejected[0].Reason)
	}
	return nil
}
