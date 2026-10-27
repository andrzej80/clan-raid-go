package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

const tracerName = "github.com/andrzej80/clan-raid-go/internal/kafka"

// headerCarrier — propagation.TextMapCarrier поверх заголовков записи Kafka: через него trace-контекст
// (traceparent) уходит вместе с сообщением и восстанавливается у консьюмера.
type headerCarrier struct {
	headers *[]kgo.RecordHeader
}

var _ propagation.TextMapCarrier = headerCarrier{}

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i := range *c.headers {
		if (*c.headers)[i].Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// injectTrace кладёт trace-контекст из ctx в заголовки записи.
func injectTrace(ctx context.Context, rec *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{&rec.Headers})
}

// extractTrace достаёт из заголовков записи trace-контекст отправителя. Запись без заголовка
// даёт ctx без родителя: такие сообщения (старые, чужие) обрабатываются как обычно.
func extractTrace(ctx context.Context, rec *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{&rec.Headers})
}

func tracer() trace.Tracer { return otel.Tracer(tracerName) }

// attrMessagingSystem помечает все span'ы пакета как относящиеся к Kafka.
var attrMessagingSystem = attribute.String("messaging.system", "kafka")

func recordAttrs(rec *kgo.Record) []attribute.KeyValue {
	return []attribute.KeyValue{
		attrMessagingSystem,
		attribute.String("messaging.destination.name", rec.Topic),
		attribute.Int("messaging.kafka.partition", int(rec.Partition)),
		attribute.Int64("messaging.kafka.offset", rec.Offset),
	}
}

// batchTrace — трассировка одной пачки записей у консьюмера.
//
// Пачка собирается из сообщений разных трейсов, поэтому у неё не может быть одного родителя:
//   - span пачки — корень нового трейса со ссылками (links) на span'ы отправителей; в него попадает
//     вся работа пачки (транзакции Postgres, запись в Redis, события в outbox);
//   - для каждой записи создаётся отдельный span в трейсе отправителя (родитель — span продюсера),
//     со ссылкой на span пачки и атрибутом batch.trace_id. Он живёт, пока пачка не обработана,
//     поэтому по hit_id в Tempo видна вся цепочка до консьюмера, а подробности ведут в трейс пачки.
//
// Лимит ссылок в SDK по умолчанию — 128, лишние отбрасываются (пачка бывает до 500 записей).
type batchTrace struct {
	batch   trace.Span
	records []trace.Span
}

// startBatchTrace открывает span пачки. ctx результата нужно передавать в обработку пачки.
func startBatchTrace(ctx context.Context, name string, records []*kgo.Record) (context.Context, *batchTrace) {
	links := make([]trace.Link, 0, len(records))
	for _, rec := range records {
		if sc := trace.SpanContextFromContext(extractTrace(context.Background(), rec)); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	ctx, span := tracer().Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links...),
		trace.WithAttributes(attribute.Int("messaging.batch.message_count", len(records))))
	return ctx, &batchTrace{batch: span}
}

// record открывает span записи в трейсе её отправителя.
func (b *batchTrace) record(rec *kgo.Record, name string, attrs ...attribute.KeyValue) {
	attrs = append(attrs, recordAttrs(rec)...)
	attrs = append(attrs, attribute.String("batch.trace_id", b.batch.SpanContext().TraceID().String()))
	_, span := tracer().Start(extractTrace(context.Background(), rec), name,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(trace.Link{SpanContext: b.batch.SpanContext()}),
		trace.WithAttributes(attrs...))
	b.records = append(b.records, span)
}

// end закрывает span'ы пачки и записей; err != nil помечает их ошибкой.
func (b *batchTrace) end(err error) {
	for _, span := range append(b.records, b.batch) {
		telemetry.RecordError(span, err)
		span.End()
	}
}
