// Package rewards начисляет награды за убитых боссов (раздел 6.4 README).
package rewards

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

const meterName = "github.com/andrzej80/clan-raid-go/internal/rewards"

const eventTypeRewardGranted = "RewardGranted"

// Значения атрибута outcome у счётчика событий.
const (
	outcomeGranted   = "granted"   // выданы новые награды
	outcomeDuplicate = "duplicate" // все награды уже были выданы: повторная доставка события
	outcomeExpired   = "expired"   // рейд истёк: наград нет
	outcomeRejected  = "rejected"  // событие нельзя обработать, оно уходит в DLQ
)

// errNotDefeated — событие defeated пришло по рейду, который в базе не defeated: данные расходятся.
var errNotDefeated = errors.New("raid is not defeated")

type metrics struct {
	events   metric.Int64Counter
	granted  metric.Int64Counter
	duration metric.Float64Histogram
}

func newMetrics(mp metric.MeterProvider) (*metrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	events, err := meter.Int64Counter("raidboss.rewards.events",
		metric.WithDescription("Raid events handled by rewards-worker, by outcome"),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("create events counter: %w", err)
	}
	granted, err := meter.Int64Counter("raidboss.rewards.granted",
		metric.WithDescription("Rewards inserted, by kind"),
		metric.WithUnit("{reward}"))
	if err != nil {
		return nil, fmt.Errorf("create granted counter: %w", err)
	}
	dur, err := meter.Float64Histogram("raidboss.rewards.duration",
		metric.WithDescription("Time to grant rewards for one defeated raid"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create duration histogram: %w", err)
	}
	return &metrics{events: events, granted: granted, duration: dur}, nil
}

func (m *metrics) event(ctx context.Context, outcome string) {
	m.events.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

type Option func(*Processor)

// WithMeterProvider подменяет провайдер метрик (для тестов).
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(p *Processor) { p.meters = mp }
}

// Processor обрабатывает события рейдов. Для defeated в одной транзакции: читает урон игроков,
// считает награды чистой функцией, вставляет их с ON CONFLICT DO NOTHING и для реально вставленных
// пишет RewardGranted в outbox. Повторная доставка события вставит 0 строк и не создаст записей в outbox.
type Processor struct {
	tx           domain.TxManager
	repo         domain.RewardsRepository
	outbox       domain.OutboxRepository
	rewardsTopic string
	meters       metric.MeterProvider
	metrics      *metrics
}

func NewProcessor(
	tx domain.TxManager,
	repo domain.RewardsRepository,
	outbox domain.OutboxRepository,
	rewardsTopic string,
	opts ...Option,
) (*Processor, error) {
	p := &Processor{tx: tx, repo: repo, outbox: outbox, rewardsTopic: rewardsTopic}
	for _, opt := range opts {
		opt(p)
	}
	m, err := newMetrics(p.meters)
	if err != nil {
		return nil, err
	}
	p.metrics = m
	return p, nil
}

// ProcessEvents обрабатывает пачку. Ошибка означает «ничего не подтверждать»: пачка будет повторена,
// повтор безопасен. События, которые нельзя обработать, возвращаются в списке отклонённых.
func (p *Processor) ProcessEvents(ctx context.Context, events []domain.RaidEvent) ([]domain.RejectedEvent, error) {
	var rejected []domain.RejectedEvent
	for i, ev := range events {
		switch ev.Kind {
		case domain.RaidEventExpired:
			p.metrics.event(ctx, outcomeExpired)
		case domain.RaidEventDefeated:
			outcome, err := p.grant(ctx, ev.RaidID)
			switch {
			case errors.Is(err, domain.ErrRaidNotFound), errors.Is(err, errNotDefeated):
				rejected = append(rejected, domain.RejectedEvent{Index: i, Reason: err.Error()})
				p.metrics.event(ctx, outcomeRejected)
			case err != nil:
				return nil, fmt.Errorf("raid %s: %w", ev.RaidID, err)
			default:
				p.metrics.event(ctx, outcome)
			}
		default:
			rejected = append(rejected, domain.RejectedEvent{Index: i, Reason: "unknown event kind " + string(ev.Kind)})
			p.metrics.event(ctx, outcomeRejected)
		}
	}
	return rejected, nil
}

func (p *Processor) grant(ctx context.Context, raidID uuid.UUID) (string, error) {
	start := time.Now()
	var inserted []domain.Reward

	err := p.tx.WithinTx(ctx, func(ctx context.Context) error {
		inserted = nil // функция может выполниться повторно при ретрае транзакции

		out, err := p.repo.RaidOutcome(ctx, raidID)
		if err != nil {
			return err
		}
		if out.Status != domain.RaidDefeated || out.KillerPlayerID == nil {
			return fmt.Errorf("%w: status %q", errNotDefeated, out.Status)
		}
		damage, err := p.repo.PlayersDamage(ctx, raidID)
		if err != nil {
			return err
		}

		byPlayer := make(map[int]int, len(damage))
		for _, d := range damage {
			byPlayer[int(d.PlayerID)] = int(d.Damage)
		}
		rewards := domain.CalcRewards(byPlayer, int(*out.KillerPlayerID))

		inserted, err = p.repo.InsertRewards(ctx, raidID, rewards)
		if err != nil {
			return err
		}
		return p.publish(ctx, raidID, inserted)
	})
	if err != nil {
		return "", err
	}

	p.metrics.duration.Record(ctx, time.Since(start).Seconds())
	for _, rw := range inserted {
		p.metrics.granted.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", string(rw.Kind))))
	}
	if len(inserted) == 0 {
		return outcomeDuplicate, nil
	}
	return outcomeGranted, nil
}

// publish пишет RewardGranted в outbox для реально вставленных наград (в порядке игрока и вида:
// RETURNING порядка не гарантирует).
func (p *Processor) publish(ctx context.Context, raidID uuid.UUID, inserted []domain.Reward) error {
	slices.SortFunc(inserted, func(a, b domain.Reward) int {
		if c := cmp.Compare(a.PlayerID, b.PlayerID); c != 0 {
			return c
		}
		return cmp.Compare(a.Kind, b.Kind)
	})
	for _, rw := range inserted {
		payload, err := proto.Marshal(&eventsV1.RewardGranted{
			RaidId:   raidID.String(),
			PlayerId: int64(rw.PlayerID),
			Kind:     string(rw.Kind),
			Amount:   int32(rw.Amount), //nolint:gosec // награды малы
		})
		if err != nil {
			return fmt.Errorf("marshal reward granted: %w", err)
		}
		headers := map[string]string{"type": eventTypeRewardGranted}
		headers = telemetry.InjectHeaders(ctx, headers)
		err = p.outbox.Add(ctx, domain.OutboxMessage{
			Topic:   p.rewardsTopic,
			Key:     strconv.Itoa(rw.PlayerID),
			Payload: payload,
			Headers: headers,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
