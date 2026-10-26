package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newMeteredProducer(t *testing.T, brokers []string, extra ...kgo.Opt) (*Producer, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	p, err := NewProducer(Config{Brokers: brokers, Topic: testTopic, MeterProvider: mp}, extra...)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})
	return p, reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// errorCount возвращает значение счётчика ошибок для данного reason (0, если точки нет).
func errorCount(t *testing.T, rm metricdata.ResourceMetrics, reason string) int64 {
	t.Helper()

	m, ok := findMetric(rm, "raidboss.kafka.produce.errors")
	if !ok {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("errors metric has type %T, want Sum[int64]", m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		if v, _ := dp.Attributes.Value(attribute.Key("reason")); v.AsString() == reason {
			total += dp.Value
		}
	}
	return total
}

// durationCount возвращает число измерений длительности с данным result.
func durationCount(t *testing.T, rm metricdata.ResourceMetrics, result string) uint64 {
	t.Helper()

	m, ok := findMetric(rm, "raidboss.kafka.produce.duration")
	if !ok {
		return 0
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("duration metric has type %T, want Histogram[float64]", m.Data)
	}
	var total uint64
	for _, dp := range hist.DataPoints {
		if v, _ := dp.Attributes.Value(attribute.Key("result")); v.AsString() == result {
			total += dp.Count
		}
	}
	return total
}

func TestProducerMetrics_SuccessIsMeasuredWithoutErrors(t *testing.T) {
	t.Parallel()

	producer, reader := newMeteredProducer(t, startCluster(t))
	for range 3 {
		if err := producer.Publish(context.Background(), newHit(uuid.New())); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	rm := collect(t, reader)
	if got := durationCount(t, rm, "ok"); got != 3 {
		t.Fatalf("ok duration samples = %d, want 3", got)
	}
	for _, reason := range []string{reasonBroker, reasonClientDeadline, reasonClientCanceled, reasonEncode} {
		if got := errorCount(t, rm, reason); got != 0 {
			t.Fatalf("errors{reason=%s} = %d, want 0", reason, got)
		}
	}
}

// Критерий этапа 2: ошибки продюсера видны в метриках.
func TestProducerMetrics_BrokerDownCountsBrokerError(t *testing.T) {
	t.Parallel()

	producer, reader := newMeteredProducer(t, []string{"127.0.0.1:1"}, kgo.RecordDeliveryTimeout(time.Second))

	if err := producer.Publish(context.Background(), newHit(uuid.New())); err == nil {
		t.Fatal("Publish must fail when the broker is unreachable")
	}

	rm := collect(t, reader)
	if got := errorCount(t, rm, reasonBroker); got != 1 {
		t.Fatalf("errors{reason=broker} = %d, want 1", got)
	}
	if got := durationCount(t, rm, "error"); got != 1 {
		t.Fatalf("error duration samples = %d, want 1", got)
	}
}

func TestProducerMetrics_ClientDeadlineIsSeparateReason(t *testing.T) {
	t.Parallel()

	producer, reader := newMeteredProducer(t, []string{"127.0.0.1:1"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	if err := producer.Publish(ctx, newHit(uuid.New())); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}

	rm := collect(t, reader)
	if got := errorCount(t, rm, reasonClientDeadline); got != 1 {
		t.Fatalf("errors{reason=client_deadline} = %d, want 1", got)
	}
	if got := errorCount(t, rm, reasonBroker); got != 0 {
		t.Fatalf("client deadline must not be counted as a broker error, got %d", got)
	}
}

func TestFailureReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", context.DeadlineExceeded, reasonClientDeadline},
		{"wrapped deadline", errors.Join(errors.New("produce"), context.DeadlineExceeded), reasonClientDeadline},
		{"canceled", context.Canceled, reasonClientCanceled},
		{"other", errors.New("connection refused"), reasonBroker},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := failureReason(tc.err); got != tc.want {
				t.Fatalf("failureReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func newLagWorker() *partitionWorker {
	w := &partitionWorker{}
	w.highWatermark.Store(-1)
	w.processed.Store(-1)
	return w
}

func TestConsumerLag(t *testing.T) {
	t.Parallel()

	tp := topicPartition{"raid.hits", 3}
	w := newLagWorker()
	c := &Consumer{partitions: map[topicPartition]*partitionWorker{tp: w}}
	reader := sdkmetric.NewManualReader()
	reg, err := registerLagGauge(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), c)
	if err != nil {
		t.Fatalf("registerLagGauge: %v", err)
	}
	t.Cleanup(func() { _ = reg.Unregister() })

	lagOf := func() (int64, bool) {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &rm); err != nil {
			t.Fatalf("Collect: %v", err)
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if g, ok := m.Data.(metricdata.Gauge[int64]); ok && m.Name == "raidboss.kafka.consumer.lag" && len(g.DataPoints) > 0 {
					return g.DataPoints[0].Value, true
				}
			}
		}
		return 0, false
	}

	if _, ok := lagOf(); ok {
		t.Fatal("lag must not be reported before the first fetch")
	}

	// Первый кусок 5..9 при конце партиции 20: необработанными остаются 15 записей.
	c.observeFetch(tp, 20, []*kgo.Record{{Offset: 5}, {Offset: 9}})
	if got, ok := lagOf(); !ok || got != 15 {
		t.Fatalf("lag = %d (reported %v), want 15", got, ok)
	}

	// Пачка обработана до offset 9 включительно, партиция выросла до 25.
	w.processed.Store(10)
	c.observeFetch(tp, 25, nil)
	if got, _ := lagOf(); got != 15 {
		t.Fatalf("lag = %d, want 15 (25-10)", got)
	}
}

func TestConsumerLagCaughtUpAtStart(t *testing.T) {
	t.Parallel()

	tp := topicPartition{"raid.hits", 0}
	w := newLagWorker()
	c := &Consumer{partitions: map[topicPartition]*partitionWorker{tp: w}}

	// Пустой ответ без обработанных записей: committed offset уже равен концу партиции.
	c.observeFetch(tp, 42, nil)
	if got, ok := w.lag(); !ok || got != 0 {
		t.Fatalf("lag = %d (known %v), want 0", got, ok)
	}
}
