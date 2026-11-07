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
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
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

	boardDuration metric.Float64Histogram
	boardErrors   metric.Int64Counter
	// freshness — от приёма удара в api (created_at) до коммита в Postgres: SLI «удар виден в игре».
	// Границы корзин — общие для гистограмм в секундах (telemetry.durationBuckets), порог SLO 2 с среди них.
	freshness metric.Float64Histogram
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
	boardDur, err := meter.Float64Histogram("raidboss.consumer.board.duration",
		metric.WithDescription("Time to write raid state to the Redis leaderboard"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create board duration histogram: %w", err)
	}
	boardErrs, err := meter.Int64Counter("raidboss.consumer.board.errors",
		metric.WithDescription("Failed writes of raid state to Redis (repaired by raidctl rebuild-cache)"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("create board errors counter: %w", err)
	}
	freshness, err := meter.Float64Histogram("raidboss.consumer.hit.freshness",
		metric.WithDescription("Time from accepting a hit in the API to committing it to PostgreSQL"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create hit freshness histogram: %w", err)
	}
	return &processorMetrics{
		hits: hits, batchSize: size, duration: dur, boardDuration: boardDur, boardErrors: boardErrs,
		freshness: freshness,
	}, nil
}

func (m *processorMetrics) addHits(ctx context.Context, outcome string, n int) {
	if n == 0 {
		return
	}
	m.hits.Add(ctx, int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
}

// raidStats — итог обработки одного рейда пачки.
type raidStats struct {
	accepted, late, duplicate int
	// totals и hpLeft заполнены, если приняты новые удары: это состояние для Redis.
	totals []domain.PlayerDamage
	hpLeft int64
	// hitsTotal — всего принято ударов в рейде с учётом этой пачки.
	hitsTotal int64
	// killer заполнен, если эта пачка добила босса.
	killer *int64
	// acceptedAt — created_at принятых ударов, для метрики свежести после коммита.
	acceptedAt []time.Time
}

// boardWriteTimeout ограничивает запись в Redis: медленный Redis не должен тормозить обработку ударов.
const boardWriteTimeout = 500 * time.Millisecond

// BoardWriter записывает состояние рейда в Redis (лидерборд, HP, pub/sub) после коммита в Postgres.
type BoardWriter interface {
	Apply(ctx context.Context, raidID uuid.UUID, totals []domain.PlayerDamage, hpLeft, hitsTotal int64) error
}

// FinishPublisher сообщает подписчикам WatchRaid об окончании рейда.
type FinishPublisher interface {
	PublishDefeated(ctx context.Context, raidID uuid.UUID, killerPlayerID int64) error
}

// HitsProcessor применяет пачки ударов к Postgres (алгоритм 6.2 README).
// Каждый рейд пачки обрабатывается в отдельной транзакции под FOR UPDATE.
type HitsProcessor struct {
	tx          domain.TxManager
	hits        domain.HitsRepository
	outbox      domain.OutboxRepository
	eventsTopic string
	board       BoardWriter
	finished    FinishPublisher
	meters      metric.MeterProvider
	metrics     *processorMetrics
	now         func() time.Time
}

type ProcessorOption func(*HitsProcessor)

// WithProcessorClock подменяет источник времени (для тестов).
func WithProcessorClock(now func() time.Time) ProcessorOption {
	return func(p *HitsProcessor) { p.now = now }
}

// WithBoard включает запись лидерборда и HP в Redis. Без неё процессор работает только с Postgres.
func WithBoard(w BoardWriter) ProcessorOption {
	return func(p *HitsProcessor) { p.board = w }
}

// WithFinishPublisher включает уведомление подписчиков WatchRaid о победе над боссом.
func WithFinishPublisher(f FinishPublisher) ProcessorOption {
	return func(p *HitsProcessor) { p.finished = f }
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
			slog.WarnContext(ctx, "hits rejected", "raid_id", group.RaidID, "hits", len(group.Hits), "err", err)
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
		p.recordFreshness(ctx, stats.acceptedAt)
		p.applyToBoard(ctx, group.RaidID, stats)
		p.publishDefeated(ctx, group.RaidID, stats)
	}

	p.metrics.duration.Record(ctx, time.Since(start).Seconds())
	return result, nil
}

// recordFreshness — после коммита: удар «виден в игре», когда он в Postgres.
func (p *HitsProcessor) recordFreshness(ctx context.Context, acceptedAt []time.Time) {
	now := p.now()
	for _, at := range acceptedAt {
		p.metrics.freshness.Record(ctx, max(now.Sub(at).Seconds(), 0))
	}
}

// applyToBoard обновляет Redis после коммита транзакции рейда (не внутри неё: откат не должен
// оставлять в Redis данные, которых нет в Postgres).
//
// Ошибка Redis пачку не повторяет и не роняет: Postgres уже зафиксирован, а повтор пачки отсечёт
// все удары по hit_id и до записи в Redis не дойдёт. Расхождение лечит raidctl rebuild-cache.
func (p *HitsProcessor) applyToBoard(ctx context.Context, raidID uuid.UUID, stats raidStats) {
	if p.board == nil || stats.accepted == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, boardWriteTimeout)
	defer cancel()

	start := time.Now()
	err := p.board.Apply(ctx, raidID, stats.totals, stats.hpLeft, stats.hitsTotal)
	p.metrics.boardDuration.Record(ctx, time.Since(start).Seconds())
	if err != nil {
		slog.WarnContext(ctx, "redis board update failed, run raidctl rebuild-cache to repair", "raid_id", raidID, "err", err)
		p.metrics.boardErrors.Add(ctx, 1)
	}
}

// publishDefeated уведомляет подписчиков после коммита. Как и запись в Redis, это best-effort:
// потерянное уведомление исправляет переподключение клиента (snapshot с итоговым статусом).
func (p *HitsProcessor) publishDefeated(ctx context.Context, raidID uuid.UUID, stats raidStats) {
	if p.finished == nil || stats.killer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, boardWriteTimeout)
	defer cancel()
	if err := p.finished.PublishDefeated(ctx, raidID, *stats.killer); err != nil {
		slog.WarnContext(ctx, "publish raid defeated failed", "raid_id", raidID, "err", err)
		p.metrics.boardErrors.Add(ctx, 1)
	}
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

		// Рейд ещё active, но scheduler мог не успеть его закрыть: опоздавшими считаем удары,
		// созданные в ends_at или позже.
		timely, afterDeadline := domain.SplitByDeadline(group.Hits, state.EndsAt)
		stats.late = len(afterDeadline)

		fresh, err := p.filterNew(ctx, timely)
		if err != nil {
			return err
		}
		stats.duplicate = len(timely) - len(fresh)
		if len(fresh) == 0 {
			return nil
		}

		out := domain.ApplyHits(state.HPLeft, fresh)
		stats.accepted, stats.late = len(out.Accepted), stats.late+len(out.Late)
		totals, hitsTotal, err := p.persist(ctx, group.RaidID, out)
		if err != nil {
			return err
		}
		stats.totals, stats.hpLeft, stats.hitsTotal = totals, out.HPLeft, hitsTotal
		for _, h := range out.Accepted {
			stats.acceptedAt = append(stats.acceptedAt, h.CreatedAt)
		}
		if out.Killing != nil {
			stats.killer = &out.Killing.PlayerID
		}
		return nil
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

// persist записывает пачку и возвращает абсолютные суммы урона затронутых игроков и общее число
// ударов рейда: оно уходит в Redis вместе с остальным состоянием.
func (p *HitsProcessor) persist(
	ctx context.Context, raidID uuid.UUID, out domain.HitsOutcome,
) ([]domain.PlayerDamage, int64, error) {
	if err := p.hits.InsertHits(ctx, out.Accepted); err != nil {
		return nil, 0, err
	}
	totals, err := p.hits.AddDamage(ctx, raidID, domain.AggregateDamage(out.Accepted))
	if err != nil {
		return nil, 0, err
	}
	// AddDamage вернул суммы только игроков пачки, поэтому общее число ударов считается отдельно.
	hitsTotal, err := p.hits.CountHits(ctx, raidID)
	if err != nil {
		return nil, 0, err
	}

	var kill *domain.Kill
	if out.Killing != nil {
		kill = &domain.Kill{PlayerID: out.Killing.PlayerID, HitID: out.Killing.ID, FinishedAt: p.now()}
	}
	if err := p.hits.UpdateHP(ctx, raidID, out.HPLeft, kill); err != nil {
		return nil, 0, err
	}
	if kill == nil {
		return totals, hitsTotal, nil
	}
	return totals, hitsTotal, p.addDefeatedEvent(ctx, raidID, kill)
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
	headers := map[string]string{"type": eventTypeRaidDefeated}
	headers = telemetry.InjectHeaders(ctx, headers)
	return p.outbox.Add(ctx, domain.OutboxMessage{
		Topic:   p.eventsTopic,
		Key:     raidID.String(),
		Payload: payload,
		Headers: headers,
	})
}
