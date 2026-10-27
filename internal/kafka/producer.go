// Package kafka содержит адаптеры к Kafka: продюсер ударов (и позже консьюмеры).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

// deliveryTimeout — сколько продюсер пытается доставить запись. Он намного короче типичного дедлайна
// клиента (секунды), чтобы при недоступном брокере Attack отвечал UNAVAILABLE, а не упирался в дедлайн
// клиента (DEADLINE_EXCEEDED). Подтверждение от здорового брокера занимает миллисекунды.
// Он же ограничивает отправку, когда у запроса нет своего дедлайна:
// без него при недоступном брокере Publish ждал бы бесконечно, а клиент должен получить UNAVAILABLE.
const deliveryTimeout = time.Second

type Config struct {
	Brokers []string
	Topic   string
	// MeterProvider необязателен: по умолчанию используется глобальный (см. telemetry.Setup).
	MeterProvider metric.MeterProvider
}

// Producer пишет удары в топик raid.hits. Ключ записи — raid_id: все удары одного рейда попадают
// в одну партицию и обрабатываются консьюмером по порядку.
type Producer struct {
	client  *kgo.Client
	topic   string
	metrics *producerMetrics
	// produce отправляет запись и ждёт подтверждения; подменяется в тестах.
	produce func(ctx context.Context, rec *kgo.Record) error
}

func NewProducer(cfg Config, extra ...kgo.Opt) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic is required")
	}

	metrics, err := newProducerMetrics(cfg.MeterProvider)
	if err != nil {
		return nil, err
	}

	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		// Подтверждение от всех реплик + идемпотентная запись (включена по умолчанию):
		// ответ клиенту «удар принят» не теряется при отказе брокера и не дублируется ретраями.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	}, extra...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	p := &Producer{client: client, topic: cfg.Topic, metrics: metrics}
	p.produce = func(ctx context.Context, rec *kgo.Record) error { return client.ProduceSync(ctx, rec).FirstErr() }
	return p, nil
}

// Publish отправляет удар и ждёт подтверждения брокера. Контекст запроса (дедлайн клиента)
// доходит до продюсера: по его отмене отправка прерывается.
func (p *Producer) Publish(ctx context.Context, hit domain.Hit) (err error) {
	start := time.Now()

	// Span продюсера — родитель для консьюмера: его контекст уходит в заголовке записи.
	ctx, span := tracer().Start(ctx, p.topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attrMessagingSystem,
			attribute.String("messaging.destination.name", p.topic),
			attribute.String("hit_id", hit.ID.String()),
			attribute.String("raid_id", hit.RaidID.String()),
		))
	defer func() {
		telemetry.RecordError(span, err)
		span.End()
	}()

	value, err := proto.Marshal(hitToEvent(hit))
	if err != nil {
		p.metrics.observe(ctx, start, reasonEncode)
		return fmt.Errorf("marshal hit event: %w", err)
	}

	rec := &kgo.Record{Key: []byte(hit.RaidID.String()), Value: value}
	injectTrace(ctx, rec)
	if err := p.produceBounded(ctx, rec); err != nil {
		p.metrics.observe(ctx, start, failureReason(err))
		return fmt.Errorf("produce to %s: %w", p.topic, err)
	}
	p.metrics.observe(ctx, start, "")
	return nil
}

// Ping проверяет доступность брокеров (для health).
func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

// Close дожидается отправки буферизованных записей и закрывает клиента.
func (p *Producer) Close(ctx context.Context) error {
	err := p.client.Flush(ctx)
	p.client.Close()
	return err
}

func hitToEvent(hit domain.Hit) *eventsV1.HitEvent {
	return &eventsV1.HitEvent{
		HitId:     hit.ID.String(),
		RaidId:    hit.RaidID.String(),
		PlayerId:  hit.PlayerID,
		ClanId:    hit.ClanID,
		Damage:    int32(hit.Damage), //nolint:gosec // урон ограничен силой игрока (≤ 1000 × 1.2 × 2)
		IsCrit:    hit.IsCrit,
		CreatedAt: timestamppb.New(hit.CreatedAt),
	}
}

// errPublishTimeout — запись не подтверждена за deliveryTimeout. Это сбой очереди, а не дедлайн клиента:
// он не оборачивает context.DeadlineExceeded, чтобы Attack ответил UNAVAILABLE и предохранитель его учёл.
var errPublishTimeout = errors.New("kafka: record not acknowledged in time")

// produceBounded ограничивает отправку deliveryTimeout независимо от внутренностей клиента. RecordDeliveryTimeout
// franz-go этого не гарантирует: пока клиент застрял на подключении или обновлении метаданных, запись
// «висит» дольше таймаута (на стенде при остановленном брокере — десятки секунд), и Attack отвечал
// DEADLINE_EXCEEDED, который предохранитель не считает сбоем очереди. Если клиент застрял, горутина с
// отправкой доживает свою попытку в фоне, а запись, дошедшая позже, безвредна: консьюмер отсекает дубль
// повтора с тем же ключом по hit_id.
func (p *Producer) produceBounded(ctx context.Context, rec *kgo.Record) error {
	pctx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- p.produce(pctx, rec) }()

	select {
	case err := <-done:
		if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			return errPublishTimeout // сработал наш таймаут, а не дедлайн клиента
		}
		return err
	case <-pctx.Done():
		if err := ctx.Err(); err != nil {
			return err // дедлайн или отмена клиента
		}
		return errPublishTimeout
	}
}
