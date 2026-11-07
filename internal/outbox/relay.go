// Package outbox переносит события из таблицы outbox в Kafka (алгоритм 6.3 README).
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/health"
)

const (
	meterName = "github.com/andrzej80/clan-raid-go/internal/outbox"

	retryBackoffMin = 100 * time.Millisecond
	retryBackoffMax = 5 * time.Second

	// gaugeTimeout ограничивает запрос размера outbox из колбэка метрик.
	gaugeTimeout = 2 * time.Second
)

// Publisher отправляет пачку записей в Kafka и возвращает ошибку, пока брокер не подтвердил все.
type Publisher interface {
	PublishBatch(ctx context.Context, msgs []domain.OutboxMessage) error
}

type Config struct {
	// BatchSize — сколько записей забирается за одну транзакцию.
	BatchSize int
	// PollInterval — пауза, когда неотправленных записей нет.
	PollInterval time.Duration
	// MeterProvider необязателен: по умолчанию используется глобальный.
	MeterProvider metric.MeterProvider
	// Heartbeat необязателен: отметка живости для /livez, ставится на каждой итерации цикла.
	Heartbeat *health.Heartbeat
	// BatchTimeout ограничивает одну пачку (блокировка, публикация, пометка, коммит); 0 —
	// DefaultBatchTimeout. Без него пачка висит, пока Postgres или Kafka не отвечают, держит
	// транзакцию и liveness ложно считает relay зависшим. По таймауту транзакция откатывается,
	// записи остаются неотправленными; уже опубликованные уйдут повторно (at-least-once).
	BatchTimeout time.Duration
}

// DefaultBatchTimeout — меньше health.DefaultMaxStall с запасом на паузу между повторами.
const DefaultBatchTimeout = 30 * time.Second

// Relay в цикле берёт пачку неотправленных записей, публикует её и помечает отправленной —
// всё в одной транзакции. Записи держит FOR UPDATE SKIP LOCKED, поэтому несколько экземпляров
// работают параллельно, не забирая одни и те же строки.
//
// Гарантия — at-least-once: если relay упал после публикации, но до COMMIT, запись уйдёт повторно.
// Получатели обязаны быть идемпотентными.
type Relay struct {
	tx      domain.TxManager
	store   domain.OutboxRelayStore
	pub     Publisher
	cfg     Config
	metrics *relayMetrics
}

func NewRelay(tx domain.TxManager, store domain.OutboxRelayStore, pub Publisher, cfg Config) (*Relay, error) {
	if cfg.BatchSize < 1 || cfg.PollInterval <= 0 {
		return nil, errors.New("outbox: batch size and poll interval must be positive")
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = DefaultBatchTimeout
	}
	r := &Relay{tx: tx, store: store, pub: pub, cfg: cfg}
	metrics, err := newRelayMetrics(cfg.MeterProvider, r.unsent)
	if err != nil {
		return nil, err
	}
	r.metrics = metrics
	return r, nil
}

func (r *Relay) unsent(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, gaugeTimeout)
	defer cancel()
	return r.store.CountUnsent(ctx)
}

// Run крутит цикл до отмены ctx. Пачка, начатая до отмены, дорабатывается до конца и коммитится:
// иначе после публикации запись осталась бы неотмеченной и ушла бы в Kafka повторно.
func (r *Relay) Run(ctx context.Context) error {
	backoff := retryBackoffMin
	for ctx.Err() == nil {
		r.cfg.Heartbeat.Beat()
		n, err := r.relayBatch(ctx)
		switch {
		case err != nil:
			slog.Error("outbox relay batch, will retry", "err", err)
			r.metrics.errors.Add(ctx, 1)
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, retryBackoffMax)
		case n == 0:
			backoff = retryBackoffMin
			if !sleep(ctx, r.cfg.PollInterval) {
				return nil
			}
		default:
			// Очередь не пуста: следующую пачку берём сразу.
			backoff = retryBackoffMin
		}
	}
	return nil
}

// relayBatch переносит одну пачку и возвращает число отправленных записей.
func (r *Relay) relayBatch(ctx context.Context) (int, error) {
	start := time.Now()
	// Контекст отвязан от сигнала остановки (см. Run), но ограничен BatchTimeout: ни таймаут
	// доставки franz-go, ни драйвер Postgres не гарантируют возврат, если сервер молчит.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.BatchTimeout)
	defer cancel()

	var sent int
	err := r.tx.WithinTx(ctx, func(ctx context.Context) error {
		sent = 0 // функция может выполниться повторно

		entries, err := r.store.LockUnsent(ctx, r.cfg.BatchSize)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}

		msgs := make([]domain.OutboxMessage, len(entries))
		ids := make([]int64, len(entries))
		for i, e := range entries {
			msgs[i], ids[i] = e.OutboxMessage, e.ID
		}
		if err := r.pub.PublishBatch(ctx, msgs); err != nil {
			return err
		}
		if err := r.store.MarkSent(ctx, ids); err != nil {
			return fmt.Errorf("records published but not marked, they will be sent again: %w", err)
		}
		sent = len(entries)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if sent > 0 {
		r.metrics.published.Add(ctx, int64(sent))
		r.metrics.duration.Record(ctx, time.Since(start).Seconds())
	}
	return sent, nil
}

// sleep ждёт d; false, если ctx отменили раньше.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type relayMetrics struct {
	published metric.Int64Counter
	errors    metric.Int64Counter
	duration  metric.Float64Histogram
}

func newRelayMetrics(mp metric.MeterProvider, unsent func(context.Context) (int64, error)) (*relayMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	published, err := meter.Int64Counter("raidboss.outbox.published",
		metric.WithDescription("Outbox records published to Kafka"),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, fmt.Errorf("create published counter: %w", err)
	}
	errs, err := meter.Int64Counter("raidboss.outbox.errors",
		metric.WithDescription("Failed relay batches (rolled back and retried)"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("create errors counter: %w", err)
	}
	dur, err := meter.Float64Histogram("raidboss.outbox.batch.duration",
		metric.WithDescription("Time to relay a non-empty batch: lock, publish, mark sent, commit"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create duration histogram: %w", err)
	}
	_, err = meter.Int64ObservableGauge("raidboss.outbox.unsent",
		metric.WithDescription("Outbox records not yet published to Kafka"),
		metric.WithUnit("{record}"),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			n, err := unsent(ctx)
			if err != nil {
				return err
			}
			o.Observe(n)
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("create unsent gauge: %w", err)
	}
	return &relayMetrics{published: published, errors: errs, duration: dur}, nil
}
