package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const (
	retryBackoffMin = 100 * time.Millisecond
	retryBackoffMax = 5 * time.Second
)

// Заголовки записи в DLQ.
const (
	headerDLQReason    = "dlq-reason"
	headerSourceTopic  = "source-topic"
	headerSourcePart   = "source-partition"
	headerSourceOffset = "source-offset"
)

// HitProcessor применяет пачку ударов. Ошибка — повторить пачку целиком (повтор идемпотентен);
// удары из BatchResult.Rejected консьюмер переносит в DLQ.
type HitProcessor interface {
	ProcessBatch(ctx context.Context, hits []domain.Hit) (domain.BatchResult, error)
}

type ConsumerConfig struct {
	Brokers      []string
	Topic        string
	DLQTopic     string
	Group        string
	BatchSize    int
	BatchTimeout time.Duration
	// MeterProvider необязателен: по умолчанию используется глобальный.
	MeterProvider metric.MeterProvider
}

// Consumer читает raid.hits группой консьюмеров. На каждую назначенную партицию — своя горутина:
// она копит пачку (до BatchSize записей или BatchTimeout), обрабатывает её и только после этого
// коммитит offset. Автокоммит выключен: гарантия at-least-once.
type Consumer struct {
	client    *kgo.Client
	processor HitProcessor
	cfg       ConsumerConfig
	metrics   *consumerMetrics

	// workCtx живёт до Close: пачка, начатая при остановке, должна дообработаться, поэтому он
	// не привязан к контексту Run (его отменяет сигнал).
	workCtx    context.Context //nolint:containedctx // общий контекст горутин партиций
	cancelWork context.CancelFunc

	mu         sync.Mutex
	partitions map[topicPartition]*partitionWorker
}

type topicPartition struct {
	topic     string
	partition int32
}

func NewConsumer(cfg ConsumerConfig, processor HitProcessor, extra ...kgo.Opt) (*Consumer, error) {
	switch {
	case len(cfg.Brokers) == 0:
		return nil, errors.New("kafka: no brokers configured")
	case cfg.Topic == "" || cfg.DLQTopic == "" || cfg.Group == "":
		return nil, errors.New("kafka: topic, dlq topic and group are required")
	case cfg.BatchSize < 1 || cfg.BatchTimeout <= 0:
		return nil, errors.New("kafka: batch size and timeout must be positive")
	}
	metrics, err := newConsumerMetrics(cfg.MeterProvider)
	if err != nil {
		return nil, err
	}

	c := &Consumer{
		processor:  processor,
		cfg:        cfg,
		metrics:    metrics,
		partitions: make(map[topicPartition]*partitionWorker),
	}
	c.workCtx, c.cancelWork = context.WithCancel(context.Background())
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		// Новая группа начинает с начала: удар, отправленный до первого запуска консьюмера, не теряется.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onRevoked),
		// DLQ пишется тем же клиентом: подтверждение от всех реплик, как у продюсера ударов.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}, extra...)

	c.client, err = kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer client: %w", err)
	}
	return c, nil
}

// Run блокируется до отмены ctx. К моменту возврата все горутины партиций остановлены:
// пачки, которые они успели начать, обработаны и закоммичены.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.stopPartitions(nil)

	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("kafka fetch", "topic", topic, "partition", partition, "err", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			c.route(topicPartition{p.Topic, p.Partition}, p.Records)
		})
	}
}

// route передаёт записи горутине партиции. Если партицию как раз отзывают, записи отбрасываются:
// offset не закоммичен, их заново прочитает новый владелец.
func (c *Consumer) route(tp topicPartition, records []*kgo.Record) {
	c.mu.Lock()
	w := c.partitions[tp]
	c.mu.Unlock()
	if w == nil {
		return
	}
	select {
	case w.records <- records:
	case <-w.quit:
	}
}

func (c *Consumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, parts := range assigned {
		for _, partition := range parts {
			tp := topicPartition{topic, partition}
			w := &partitionWorker{
				consumer: c,
				tp:       tp,
				records:  make(chan []*kgo.Record),
				quit:     make(chan struct{}),
				done:     make(chan struct{}),
			}
			c.partitions[tp] = w
			go w.run(c.workCtx) //nolint:contextcheck // горутина партиций живёт дольше колбэка ребаланса
		}
	}
}

// onRevoked останавливает горутины отзываемых партиций и ждёт их: текущая пачка дообрабатывается
// и коммитится до того, как партиция уйдёт к другому участнику группы.
func (c *Consumer) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.stopPartitions(revoked)
}

// stopPartitions останавливает указанные партиции (nil — все) и дожидается завершения горутин.
func (c *Consumer) stopPartitions(only map[string][]int32) {
	var stopping []*partitionWorker

	c.mu.Lock()
	for tp, w := range c.partitions {
		if only != nil && !slices.Contains(only[tp.topic], tp.partition) {
			continue
		}
		delete(c.partitions, tp)
		stopping = append(stopping, w)
	}
	c.mu.Unlock()

	for _, w := range stopping {
		close(w.quit)
	}
	for _, w := range stopping {
		<-w.done
	}
}

// Ping проверяет доступность брокеров.
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }

// Close выходит из группы и закрывает клиента. Вызывать после возврата Run.
func (c *Consumer) Close() {
	c.stopPartitions(nil)
	c.cancelWork()
	c.client.Close()
}

type partitionWorker struct {
	consumer *Consumer
	tp       topicPartition
	records  chan []*kgo.Record
	quit     chan struct{} // закрывается при отзыве партиции или остановке
	done     chan struct{} // закрывается, когда горутина завершилась
}

func (w *partitionWorker) run(ctx context.Context) {
	defer close(w.done)

	cfg := w.consumer.cfg
	var (
		buf   []*kgo.Record
		timer *time.Timer
		fire  <-chan time.Time
	)
	flush := func() {
		if timer != nil {
			timer.Stop()
			timer, fire = nil, nil
		}
		if len(buf) == 0 {
			return
		}
		w.handle(ctx, buf)
		buf = nil
	}

	for {
		select {
		case recs := <-w.records:
			buf = append(buf, recs...)
			switch {
			case len(buf) >= cfg.BatchSize:
				flush()
			case timer == nil:
				timer = time.NewTimer(cfg.BatchTimeout)
				fire = timer.C
			}
		case <-fire:
			flush()
		case <-w.quit:
			flush()
			return
		case <-ctx.Done():
			return
		}
	}
}

// handle обрабатывает накопленные записи кусками не больше BatchSize.
func (w *partitionWorker) handle(ctx context.Context, records []*kgo.Record) {
	size := w.consumer.cfg.BatchSize
	for len(records) > 0 {
		n := min(size, len(records))
		if !w.handleBatch(ctx, records[:n]) {
			return // пачка не обработана: offset не двигаем, её заново прочитает владелец партиции
		}
		records = records[n:]
	}
}

// handleBatch: разбор записей, обработка в Postgres, отправка негодных в DLQ, коммит offset'а.
// Возвращает false, если пачку пришлось бросить (остановка) — тогда offset не коммитится.
func (w *partitionWorker) handleBatch(ctx context.Context, records []*kgo.Record) bool {
	c := w.consumer
	hits := make([]domain.Hit, 0, len(records))
	bySource := make(map[uuid.UUID]*kgo.Record, len(records))

	for _, rec := range records {
		hit, err := decodeHit(rec.Value)
		if err != nil {
			if !w.sendDLQ(ctx, rec, dlqReasonDecode, err.Error()) {
				return false
			}
			continue
		}
		hits = append(hits, hit)
		bySource[hit.ID] = rec
	}

	if len(hits) > 0 {
		result, ok := w.processWithRetry(ctx, hits)
		if !ok {
			return false
		}
		for _, rejected := range result.Rejected {
			for _, hit := range rejected.Hits {
				if !w.sendDLQ(ctx, bySource[hit.ID], dlqReasonRejected, rejected.Reason) {
					return false
				}
			}
		}
	}

	// Коммит — только после того, как эффект записан в БД. Ошибка коммита не фатальна: следующая
	// пачка закоммитит дальше, а при повторной доставке отработает идемпотентность.
	last := records[len(records)-1]
	if err := c.client.CommitRecords(ctx, last); err != nil && ctx.Err() == nil {
		slog.Warn("kafka commit", "topic", w.tp.topic, "partition", w.tp.partition, "err", err)
	}
	return true
}

// processWithRetry повторяет пачку с экспоненциальной паузой, пока Postgres не ответит.
// Остановка (отмена контекста или отзыв партиции) прерывает повторы.
func (w *partitionWorker) processWithRetry(ctx context.Context, hits []domain.Hit) (domain.BatchResult, bool) {
	backoff := retryBackoffMin
	for {
		result, err := w.consumer.processor.ProcessBatch(ctx, hits)
		if err == nil {
			return result, true
		}
		w.consumer.metrics.batchRetries.Add(ctx, 1)
		slog.Error("process batch, will retry",
			"topic", w.tp.topic, "partition", w.tp.partition, "hits", len(hits), "err", err)
		if !w.wait(ctx, backoff) {
			return domain.BatchResult{}, false
		}
		backoff = min(backoff*2, retryBackoffMax)
	}
}

// wait ждёт d; false — если за это время пришла остановка.
func (w *partitionWorker) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-w.quit:
		return false
	}
}

// sendDLQ кладёт исходные байты записи в DLQ-топик с причиной в заголовке и ждёт подтверждения.
// Пока DLQ недоступен, повторяет: потерять сообщение молча нельзя.
func (w *partitionWorker) sendDLQ(ctx context.Context, src *kgo.Record, reason, details string) bool {
	c := w.consumer
	dlq := &kgo.Record{
		Topic: c.cfg.DLQTopic,
		Key:   src.Key,
		Value: src.Value,
		Headers: []kgo.RecordHeader{
			{Key: headerDLQReason, Value: []byte(reason + ": " + details)},
			{Key: headerSourceTopic, Value: []byte(src.Topic)},
			{Key: headerSourcePart, Value: []byte(strconv.Itoa(int(src.Partition)))},
			{Key: headerSourceOffset, Value: []byte(strconv.FormatInt(src.Offset, 10))},
		},
	}

	backoff := retryBackoffMin
	for {
		err := c.client.ProduceSync(ctx, dlq).FirstErr()
		if err == nil {
			c.metrics.dlq.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
			slog.Warn("record sent to dlq", "reason", reason, "details", details,
				"topic", src.Topic, "partition", src.Partition, "offset", src.Offset)
			return true
		}
		slog.Error("send to dlq, will retry", "err", err)
		if !w.wait(ctx, backoff) {
			return false
		}
		backoff = min(backoff*2, retryBackoffMax)
	}
}

// decodeHit разбирает и проверяет HitEvent. Любая ошибка — повод отправить запись в DLQ.
func decodeHit(value []byte) (domain.Hit, error) {
	var ev eventsV1.HitEvent
	if err := proto.Unmarshal(value, &ev); err != nil {
		return domain.Hit{}, fmt.Errorf("unmarshal hit event: %w", err)
	}
	hitID, err := uuid.Parse(ev.GetHitId())
	if err != nil {
		return domain.Hit{}, fmt.Errorf("hit_id: %w", err)
	}
	raidID, err := uuid.Parse(ev.GetRaidId())
	if err != nil {
		return domain.Hit{}, fmt.Errorf("raid_id: %w", err)
	}
	if ev.GetPlayerId() <= 0 || ev.GetDamage() <= 0 || ev.GetCreatedAt() == nil {
		return domain.Hit{}, errors.New("player_id, damage and created_at must be set")
	}
	return domain.Hit{
		ID:        hitID,
		RaidID:    raidID,
		PlayerID:  ev.GetPlayerId(),
		ClanID:    ev.GetClanId(),
		Damage:    int(ev.GetDamage()),
		IsCrit:    ev.GetIsCrit(),
		CreatedAt: ev.GetCreatedAt().AsTime(),
	}, nil
}
