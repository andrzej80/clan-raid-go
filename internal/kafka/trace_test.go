package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// Тесты меняют глобальные провайдер и propagator OTel, поэтому не параллельны: параллельные тесты
// пакета стартуют только после всех последовательных.
func setupTracing(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return sr
}

// producerSpan имитирует span продюсера и возвращает запись с его контекстом в заголовках.
func producerSpan(t *testing.T, value []byte) (*kgo.Record, trace.SpanContext) {
	t.Helper()
	ctx, span := tracer().Start(context.Background(), "producer")
	rec := &kgo.Record{Topic: "raid.hits", Value: value}
	injectTrace(ctx, rec)
	span.End()
	return rec, span.SpanContext()
}

func hitValue(t *testing.T, hitID, raidID uuid.UUID) []byte {
	t.Helper()
	b, err := proto.Marshal(&eventsV1.HitEvent{
		HitId: hitID.String(), RaidId: raidID.String(), PlayerId: 1, Damage: 10, CreatedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestHeaderCarrierRoundTrip(t *testing.T) { //nolint:paralleltest // меняет глобальный провайдер OTel
	setupTracing(t)

	rec, sent := producerSpan(t, nil)
	if len(rec.Headers) != 1 || rec.Headers[0].Key != "traceparent" {
		t.Fatalf("headers = %+v, want a single traceparent", rec.Headers)
	}

	got := trace.SpanContextFromContext(extractTrace(context.Background(), rec))
	if got.TraceID() != sent.TraceID() || got.SpanID() != sent.SpanID() || !got.IsRemote() {
		t.Fatalf("extracted %v, want remote %v", got, sent)
	}

	// Повторная вставка заменяет заголовок, а не дублирует его.
	injectTrace(trace.ContextWithSpanContext(context.Background(), sent), rec)
	if len(rec.Headers) != 1 {
		t.Fatalf("headers duplicated: %+v", rec.Headers)
	}
}

func TestExtractTraceWithoutHeader(t *testing.T) { //nolint:paralleltest // меняет глобальный провайдер OTel
	setupTracing(t)

	ctx := extractTrace(context.Background(), &kgo.Record{})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("span context must be empty for a record without traceparent")
	}
}

type ctxCapturingProcessor struct {
	span trace.SpanContext
	err  error
}

func (p *ctxCapturingProcessor) ProcessBatch(ctx context.Context, _ []domain.Hit) (domain.BatchResult, error) {
	p.span = trace.SpanContextFromContext(ctx)
	return domain.BatchResult{}, p.err
}

// collectHitSpans возвращает span пачки и span'ы записей по hit_id.
func collectHitSpans(sr *tracetest.SpanRecorder) (batch tracetest.SpanStub, records map[string]tracetest.SpanStub) {
	records = map[string]tracetest.SpanStub{}
	for _, s := range sr.Ended() {
		stub := tracetest.SpanStubFromReadOnlySpan(s)
		switch stub.Name {
		case "raid.hits process batch":
			batch = stub
		case "raid.hits process hit":
			for _, a := range stub.Attributes {
				if a.Key == "hit_id" {
					records[a.Value.AsString()] = stub
				}
			}
		}
	}
	return batch, records
}

func TestHitsHandlerTracesBatch(t *testing.T) { //nolint:paralleltest // меняет глобальный провайдер OTel
	sr := setupTracing(t)

	hitA, hitB := uuid.New(), uuid.New()
	recA, sentA := producerSpan(t, hitValue(t, hitA, uuid.New()))
	recB, sentB := producerSpan(t, hitValue(t, hitB, uuid.New()))
	recNoTrace := &kgo.Record{Topic: "raid.hits", Value: hitValue(t, uuid.New(), uuid.New())}

	proc := &ctxCapturingProcessor{}
	_, err := hitsHandler{processor: proc}.HandleBatch(context.Background(), []*kgo.Record{recA, recB, recNoTrace})
	if err != nil {
		t.Fatalf("HandleBatch: %v", err)
	}

	batch, records := collectHitSpans(sr)

	// Span пачки — корень нового трейса со ссылками на продюсеров (запись без заголовка ссылки не даёт).
	if batch.Name == "" {
		t.Fatal("batch span not recorded")
	}
	if batch.Parent.IsValid() {
		t.Fatalf("batch span must be a root, parent = %v", batch.Parent)
	}
	linked := map[trace.SpanID]bool{}
	for _, l := range batch.Links {
		linked[l.SpanContext.SpanID()] = true
	}
	if len(batch.Links) != 2 || !linked[sentA.SpanID()] || !linked[sentB.SpanID()] {
		t.Fatalf("batch links = %v, want producers %v and %v", batch.Links, sentA.SpanID(), sentB.SpanID())
	}

	// Обработчик получил контекст пачки: работа в Postgres попадёт в её трейс.
	if got := proc.span; got.SpanID() != batch.SpanContext.SpanID() {
		t.Fatalf("processor ctx span = %v, want batch span %v", got.SpanID(), batch.SpanContext.SpanID())
	}

	// Span записи продолжает трейс продюсера и ссылается на пачку.
	for hitID, want := range map[uuid.UUID]trace.SpanContext{hitA: sentA, hitB: sentB} {
		rs, ok := records[hitID.String()]
		if !ok {
			t.Fatalf("no record span for hit %s", hitID)
		}
		if rs.Parent.SpanID() != want.SpanID() || rs.SpanContext.TraceID() != want.TraceID() {
			t.Errorf("hit %s: parent = %v, want producer %v", hitID, rs.Parent, want)
		}
		if len(rs.Links) != 1 || rs.Links[0].SpanContext.SpanID() != batch.SpanContext.SpanID() {
			t.Errorf("hit %s: links = %v, want link to batch", hitID, rs.Links)
		}
	}
}

func TestHitsHandlerMarksSpansOnError(t *testing.T) { //nolint:paralleltest // меняет глобальный провайдер OTel
	sr := setupTracing(t)

	rec, _ := producerSpan(t, hitValue(t, uuid.New(), uuid.New()))
	boom := errors.New("boom")
	if _, err := (hitsHandler{processor: &ctxCapturingProcessor{err: boom}}).
		HandleBatch(context.Background(), []*kgo.Record{rec}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}

	for _, s := range sr.Ended() {
		if s.Name() == "producer" {
			continue
		}
		if s.Status().Code != codes.Error {
			t.Errorf("span %q status = %v, want Error", s.Name(), s.Status().Code)
		}
	}
}

func TestRaidEventsHandlerContinuesOutboxTrace(t *testing.T) { //nolint:paralleltest // меняет глобальный провайдер OTel
	sr := setupTracing(t)

	raidID := uuid.New()
	value, err := proto.Marshal(defeatedEvent(raidID))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rec, sent := producerSpan(t, value) // traceparent берётся из заголовков outbox, как у relay

	if _, err := (raidEventsHandler{processor: noopEvents{}}).
		HandleBatch(context.Background(), []*kgo.Record{rec}); err != nil {
		t.Fatalf("HandleBatch: %v", err)
	}

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "raid.events process event" {
			found = true
			if s.Parent().SpanID() != sent.SpanID() {
				t.Errorf("parent = %v, want %v", s.Parent().SpanID(), sent.SpanID())
			}
		}
	}
	if !found {
		t.Fatal("event span not recorded")
	}
}

type noopEvents struct{}

func (noopEvents) ProcessEvents(context.Context, []domain.RaidEvent) ([]domain.RejectedEvent, error) {
	return nil, nil
}

func TestProducerPropagatesTrace(t *testing.T) { //nolint:paralleltest // меняет глобальный провайдер OTel
	sr := setupTracing(t)

	brokers := startCluster(t)
	p := newProducer(t, brokers)

	hit := newHit(uuid.New())
	if err := p.Publish(context.Background(), hit); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var pub sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == testTopic+" publish" {
			pub = s
		}
	}
	if pub == nil {
		t.Fatal("publish span not recorded")
	}

	rec := consume(t, brokers, 1)[0]
	got := trace.SpanContextFromContext(extractTrace(context.Background(), rec))
	if got.TraceID() != pub.SpanContext().TraceID() || got.SpanID() != pub.SpanContext().SpanID() {
		t.Fatalf("record carries %v, want publish span %v", got, pub.SpanContext())
	}
}
