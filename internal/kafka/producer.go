// Package kafka содержит адаптеры к Kafka: продюсер ударов (и позже консьюмеры).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// deliveryTimeout ограничивает отправку, когда у запроса нет своего дедлайна:
// без него при недоступном брокере Publish ждал бы бесконечно, а клиент должен получить UNAVAILABLE.
const deliveryTimeout = 5 * time.Second

type Config struct {
	Brokers []string
	Topic   string
}

// Producer пишет удары в топик raid.hits. Ключ записи — raid_id: все удары одного рейда попадают
// в одну партицию и обрабатываются консьюмером по порядку.
type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(cfg Config, extra ...kgo.Opt) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic is required")
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
	return &Producer{client: client, topic: cfg.Topic}, nil
}

// Publish отправляет удар и ждёт подтверждения брокера. Контекст запроса (дедлайн клиента)
// доходит до продюсера: по его отмене отправка прерывается.
func (p *Producer) Publish(ctx context.Context, hit domain.Hit) error {
	value, err := proto.Marshal(hitToEvent(hit))
	if err != nil {
		return fmt.Errorf("marshal hit event: %w", err)
	}

	rec := &kgo.Record{Key: []byte(hit.RaidID.String()), Value: value}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", p.topic, err)
	}
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
