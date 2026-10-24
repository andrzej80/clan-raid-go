package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// Publisher отправляет записи outbox в Kafka: топик, ключ и заголовки берутся из самой записи.
// В отличие от Producer он не привязан к одному топику.
type Publisher struct {
	client *kgo.Client
}

func NewPublisher(brokers []string, extra ...kgo.Opt) (*Publisher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		// Как у Producer: подтверждение от всех реплик и идемпотентная запись. Запись помечается
		// отправленной только после подтверждения брокера.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	}, extra...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new publisher client: %w", err)
	}
	return &Publisher{client: client}, nil
}

// PublishBatch отправляет записи и ждёт подтверждения всех. Ошибка не говорит, какие записи
// дошли: при повторе часть из них может быть опубликована дважды (at-least-once).
func (p *Publisher) PublishBatch(ctx context.Context, msgs []domain.OutboxMessage) error {
	records := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		records[i] = &kgo.Record{
			Topic:   m.Topic,
			Key:     []byte(m.Key),
			Value:   m.Payload,
			Headers: recordHeaders(m.Headers),
		}
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("publish %d records: %w", len(records), err)
	}
	return nil
}

// recordHeaders сортирует заголовки по ключу: состав записи не зависит от порядка обхода map.
func recordHeaders(headers map[string]string) []kgo.RecordHeader {
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	out := make([]kgo.RecordHeader, len(keys))
	for i, k := range keys {
		out[i] = kgo.RecordHeader{Key: k, Value: []byte(headers[k])}
	}
	return out
}

// Ping проверяет доступность брокеров.
func (p *Publisher) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

// Close дожидается отправки буферизованных записей и закрывает клиента.
func (p *Publisher) Close(ctx context.Context) error {
	err := p.client.Flush(ctx)
	p.client.Close()
	return err
}
