package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/health"
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

// BatchHandler обрабатывает пачку записей одной партиции. Ошибка — повторить пачку целиком
// (повтор должен быть идемпотентным). Записи из результата консьюмер переносит в DLQ и только
// после этого коммитит offset.
type BatchHandler interface {
	HandleBatch(ctx context.Context, records []*kgo.Record) ([]DLQRecord, error)
}

// DLQRecord — запись, которую нельзя обработать: она уходит в DLQ с причиной.
type DLQRecord struct {
	Record  *kgo.Record
	Reason  string // короткая причина, попадает в метку метрики
	Details string
}

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
	// Heartbeat необязателен: отметка живости для /livez. Отмечаются цикл опроса (не реже
	// pollTimeout), каждая пачка и каждая попытка повтора, поэтому долгий повтор при недоступной
	// БД не выглядит зависанием.
	Heartbeat *health.Heartbeat
}

// pollTimeout ограничивает одно ожидание PollFetches: без записей в топике цикл опроса иначе
// стоял бы бесконечно и не отмечал живость.
const pollTimeout = 5 * time.Second

// Consumer читает raid.hits группой консьюмеров. На каждую назначенную партицию — своя горутина:
// она копит пачку (до BatchSize записей или BatchTimeout), обрабатывает её и только после этого
// коммитит offset. Автокоммит выключен: гарантия at-least-once.
type Consumer struct {
	client  *kgo.Client
	handler BatchHandler
	cfg     ConsumerConfig
	metrics *consumerMetrics
	lagReg  metric.Registration
	// pollTimeout — длительность одного ожидания PollFetches; в тестах короче.
	pollTimeout time.Duration

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

// NewConsumer создаёт консьюмер ударов: записи разбираются как HitEvent и передаются processor.
func NewConsumer(cfg ConsumerConfig, processor HitProcessor, extra ...kgo.Opt) (*Consumer, error) {
	return NewBatchConsumer(cfg, hitsHandler{processor: processor}, extra...)
}

// NewBatchConsumer создаёт консьюмер с произвольным обработчиком пачек.
func NewBatchConsumer(cfg ConsumerConfig, handler BatchHandler, extra ...kgo.Opt) (*Consumer, error) {
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
		handler:     handler,
		cfg:         cfg,
		metrics:     metrics,
		partitions:  make(map[topicPartition]*partitionWorker),
		pollTimeout: pollTimeout,
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

	if c.lagReg, err = registerLagGauge(cfg.MeterProvider, c); err != nil {
		return nil, err
	}

	c.client, err = kgo.NewClient(opts...)
	if err != nil {
		c.unregisterLag()
		return nil, fmt.Errorf("kafka: new consumer client: %w", err)
	}
	return c, nil
}

// Run блокируется до отмены ctx. К моменту возврата все горутины партиций остановлены:
// пачки, которые они успели начать, обработаны и закоммичены.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.stopPartitions(nil)

	for {
		pollCtx, cancel := context.WithTimeout(ctx, c.pollTimeout)
		fetches := c.client.PollFetches(pollCtx)
		cancel()
		c.cfg.Heartbeat.Beat()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) {
				return // истёк pollTimeout: новых записей нет, это не ошибка
			}
			slog.Error("kafka fetch", "topic", topic, "partition", partition, "err", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tp := topicPartition{p.Topic, p.Partition}
			c.observeFetch(tp, p.HighWatermark, p.Records)
			if len(p.Records) == 0 {
				return
			}
			c.route(tp, p.Records)
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
			w.highWatermark.Store(-1)
			w.processed.Store(-1)
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
	c.unregisterLag()
	c.client.Close()
}

type partitionWorker struct {
	consumer *Consumer
	tp       topicPartition
	records  chan []*kgo.Record
	quit     chan struct{} // закрывается при отзыве партиции или остановке
	done     chan struct{} // закрывается, когда горутина завершилась

	// Для метрики лага. Оба значения — "следующий offset"; -1 — ещё неизвестно.
	highWatermark atomic.Int64 // конец партиции по последнему ответу брокера
	processed     atomic.Int64 // offset, с которого начнётся необработанное
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

// handleBatch: обработка пачки, отправка негодных записей в DLQ, коммит offset'а.
// Возвращает false, если пачку пришлось бросить (остановка) — тогда offset не коммитится.
func (w *partitionWorker) handleBatch(ctx context.Context, records []*kgo.Record) bool {
	dead, ok := w.handleWithRetry(ctx, records)
	if !ok {
		return false
	}
	for _, d := range dead {
		if !w.sendDLQ(ctx, d.Record, d.Reason, d.Details) {
			return false
		}
	}

	last := records[len(records)-1]
	w.processed.Store(last.Offset + 1)

	// Коммит — только после того, как эффект записан в БД. Ошибка коммита не фатальна: следующая
	// пачка закоммитит дальше, а при повторной доставке отработает идемпотентность.
	if err := w.consumer.client.CommitRecords(ctx, last); err != nil && ctx.Err() == nil {
		slog.Warn("kafka commit", "topic", w.tp.topic, "partition", w.tp.partition, "err", err)
	}
	return true
}

// handleWithRetry повторяет пачку с экспоненциальной паузой, пока обработчик не справится.
// Остановка (отмена контекста или отзыв партиции) прерывает повторы.
func (w *partitionWorker) handleWithRetry(ctx context.Context, records []*kgo.Record) ([]DLQRecord, bool) {
	backoff := retryBackoffMin
	for {
		w.consumer.cfg.Heartbeat.Beat()
		dead, err := w.consumer.handler.HandleBatch(ctx, records)
		if err == nil {
			return dead, true
		}
		w.consumer.metrics.batchRetries.Add(ctx, 1)
		slog.Error("handle batch, will retry",
			"topic", w.tp.topic, "partition", w.tp.partition, "records", len(records), "err", err)
		if !w.wait(ctx, backoff) {
			return nil, false
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
		c.cfg.Heartbeat.Beat()
		err := c.client.ProduceSync(ctx, dlq).FirstErr()
		if err == nil {
			c.metrics.dlq.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
			// Лог привязан к трейсу отправителя: из DLQ-записи видно, какой запрос её породил.
			slog.WarnContext(extractTrace(ctx, src), "record sent to dlq", "reason", reason, "details", details,
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

// observeFetch запоминает конец партиции из ответа брокера (для метрики лага). Для партиции, у которой
// ещё не было обработанных записей, точкой отсчёта служит начало первого полученного куска; пустой
// ответ значит, что читать нечего и консьюмер догнал конец.
func (c *Consumer) observeFetch(tp topicPartition, highWatermark int64, records []*kgo.Record) {
	c.mu.Lock()
	w := c.partitions[tp]
	c.mu.Unlock()
	if w == nil {
		return
	}
	w.highWatermark.Store(highWatermark)
	if w.processed.Load() >= 0 {
		return
	}
	if len(records) > 0 {
		w.processed.CompareAndSwap(-1, records[0].Offset)
	} else {
		w.processed.CompareAndSwap(-1, highWatermark)
	}
}

// lag возвращает отставание партиции в записях; false, пока оно неизвестно.
func (w *partitionWorker) lag() (int64, bool) {
	hwm, processed := w.highWatermark.Load(), w.processed.Load()
	if hwm < 0 || processed < 0 {
		return 0, false
	}
	return max(hwm-processed, 0), true
}

func (c *Consumer) unregisterLag() {
	if err := c.lagReg.Unregister(); err != nil {
		slog.Warn("unregister kafka lag gauge", "err", err)
	}
}
