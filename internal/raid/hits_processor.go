package raid

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const processorMeterName = "github.com/andrzej80/clan-raid-go/internal/raid"

// Значения атрибута outcome у счётчика ударов.
const (
	outcomeAccepted  = "accepted"
	outcomeLate      = "late"
	outcomeDuplicate = "duplicate"
	outcomeRejected  = "rejected"
)

const eventTypeRaidDefeated = "RaidDefeated"

type processorMetrics struct {
	hits      metric.Int64Counter
	batchSize metric.Int64Histogram
	duration  metric.Float64Histogram
}

func newProcessorMetrics(mp metric.MeterProvider) (*processorMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(processorMeterName)

	hits, err := meter.Int64Counter("raidboss.consumer.hits",
		metric.WithDescription("Hits handled by the consumer, by outcome"),
		metric.WithUnit("{hit}"))
	if err != nil {
		return nil, fmt.Errorf("create hits counter: %w", err)
	}
	size, err := meter.Int64Histogram("raidboss.consumer.batch.size",
		metric.WithDescription("Number of hits in a processed batch"),
		metric.WithUnit("{hit}"))
	if err != nil {
		return nil, fmt.Errorf("create batch size histogram: %w", err)
	}
	dur, err := meter.Float64Histogram("raidboss.consumer.batch.duration",
		metric.WithDescription("Time to apply a batch to PostgreSQL"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create batch duration histogram: %w", err)
	}
	return &processorMetrics{hits: hits, batchSize: size, duration: dur}, nil
}

func (m *processorMetrics) addHits(ctx context.Context, outcome string, n int) {
	if n == 0 {
		return
	}
	m.hits.Add(ctx, int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
}

// raidStats — счётчики по одному рейду пачки.
type raidStats struct {
	accepted, late, duplicate int
}

// HitsProcessor применяет пачки ударов к Postgres (алгоритм 6.2 README).
// Каждый рейд пачки обрабатывается в отдельной транзакции под FOR UPDATE.
type HitsProcessor struct {
	tx          domain.TxManager
	hits        domain.HitsRepository
	outbox      domain.OutboxRepository
	eventsTopic string
	meters      metric.MeterProvider
	metrics     *processorMetrics
	now         func() time.Time
}

type ProcessorOption func(*HitsProcessor)

// WithProcessorClock подменяет источник времени (для тестов).
func WithProcessorClock(now func() time.Time) ProcessorOption {
	return func(p *HitsProcessor) { p.now = now }
}

// WithProcessorMeterProvider подменяет провайдер метрик (для тестов).
func WithProcessorMeterProvider(mp metric.MeterProvider) ProcessorOption {
	return func(p *HitsProcessor) { p.meters = mp }
}

func NewHitsProcessor(
	tx domain.TxManager,
	hits domain.HitsRepository,
	outbox domain.OutboxRepository,
	eventsTopic string,
	opts ...ProcessorOption,
) (*HitsProcessor, error) {
	p := &HitsProcessor{
		tx: tx, hits: hits, outbox: outbox, eventsTopic: eventsTopic,
		now: time.Now,
	}
	for _, opt := range opts {
		opt(p)
	}
	metrics, err := newProcessorMetrics(p.meters)
	if err != nil {
		return nil, err
	}
	p.metrics = metrics
	return p, nil
}

// ProcessBatch применяет пачку. Ошибка означает «ничего не подтверждать»: пачку нужно повторить,
// повтор безопасен — уже записанные удары отсекаются по hit_id. Неисправимые удары возвращаются
// в BatchResult.Rejected, и вызывающий обязан убрать их в DLQ до коммита offset'а.
func (p *HitsProcessor) ProcessBatch(ctx context.Context, batch []domain.Hit) (domain.BatchResult, error) {
	start := time.Now()
	var result domain.BatchResult

	unique := domain.DedupHits(batch)
	p.metrics.addHits(ctx, outcomeDuplicate, len(batch)-len(unique))
	p.metrics.batchSize.Record(ctx, int64(len(batch)))

	for _, group := range domain.GroupByRaid(unique) {
		stats, err := p.processRaid(ctx, group)
		switch {
		case errors.Is(err, domain.ErrPlayerNotFound), errors.Is(err, domain.ErrRaidNotFound):
			// Повтор не поможет: рейда или игрока нет. API проверяет их до отправки в очередь,
			// поэтому сюда попадают только битые данные.
			slog.Warn("hits rejected", "raid_id", group.RaidID, "hits", len(group.Hits), "err", err)
			p.metrics.addHits(ctx, outcomeRejected, len(group.Hits))
			result.Rejected = append(result.Rejected, domain.RejectedHits{
				RaidID: group.RaidID, Hits: group.Hits, Reason: err.Error(),
			})
			continue
		case err != nil:
			return result, fmt.Errorf("raid %s: %w", group.RaidID, err)
		}
		p.metrics.addHits(ctx, outcomeAccepted, stats.accepted)
		p.metrics.addHits(ctx, outcomeLate, stats.late)
		p.metrics.addHits(ctx, outcomeDuplicate, stats.duplicate)
	}

	p.metrics.duration.Record(ctx, time.Since(start).Seconds())
	return result, nil
}

func (p *HitsProcessor) processRaid(ctx context.Context, group domain.RaidHits) (raidStats, error) {
	var stats raidStats
	err := p.tx.WithinTx(ctx, func(ctx context.Context) error {
		stats = raidStats{} // функция может выполниться повторно при ретрае транзакции

		state, err := p.hits.LockRaid(ctx, group.RaidID)
		if err != nil {
			return err
		}
		if state.Status != domain.RaidActive {
			stats.late = len(group.Hits)
			return nil
		}

		fresh, err := p.filterNew(ctx, group.Hits)
		if err != nil {
			return err
		}
		stats.duplicate = len(group.Hits) - len(fresh)
		if len(fresh) == 0 {
			return nil
		}

		out := domain.ApplyHits(state.HPLeft, fresh)
		stats.accepted, stats.late = len(out.Accepted), len(out.Late)
		return p.persist(ctx, group.RaidID, out)
	})
	return stats, err
}

// filterNew отсекает удары, уже записанные предыдущей (повторно пришедшей) обработкой.
func (p *HitsProcessor) filterNew(ctx context.Context, hits []domain.Hit) ([]domain.Hit, error) {
	ids := make([]uuid.UUID, len(hits))
	for i, h := range hits {
		ids[i] = h.ID
	}
	existing, err := p.hits.ExistingHitIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(existing) == 0 {
		return hits, nil
	}
	fresh := make([]domain.Hit, 0, len(hits)-len(existing))
	for _, h := range hits {
		if _, done := existing[h.ID]; !done {
			fresh = append(fresh, h)
		}
	}
	return fresh, nil
}

func (p *HitsProcessor) persist(ctx context.Context, raidID uuid.UUID, out domain.HitsOutcome) error {
	if err := p.hits.InsertHits(ctx, out.Accepted); err != nil {
		return err
	}
	if _, err := p.hits.AddDamage(ctx, raidID, domain.AggregateDamage(out.Accepted)); err != nil {
		return err
	}

	var kill *domain.Kill
	if out.Killing != nil {
		kill = &domain.Kill{PlayerID: out.Killing.PlayerID, HitID: out.Killing.ID, FinishedAt: p.now()}
	}
	if err := p.hits.UpdateHP(ctx, raidID, out.HPLeft, kill); err != nil {
		return err
	}
	if kill == nil {
		return nil
	}
	return p.addDefeatedEvent(ctx, raidID, kill)
}

func (p *HitsProcessor) addDefeatedEvent(ctx context.Context, raidID uuid.UUID, kill *domain.Kill) error {
	payload, err := proto.Marshal(&eventsV1.RaidEvent{
		RaidId:     raidID.String(),
		OccurredAt: timestamppb.New(kill.FinishedAt),
		Event: &eventsV1.RaidEvent_Defeated{Defeated: &eventsV1.RaidDefeated{
			KillerPlayerId: kill.PlayerID,
			KillerHitId:    kill.HitID.String(),
		}},
	})
	if err != nil {
		return fmt.Errorf("marshal raid defeated event: %w", err)
	}
	return p.outbox.Add(ctx, domain.OutboxMessage{
		Topic:   p.eventsTopic,
		Key:     raidID.String(),
		Payload: payload,
		Headers: map[string]string{"type": eventTypeRaidDefeated},
	})
}
