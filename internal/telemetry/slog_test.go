package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestWithTraceIDs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(WithTraceIDs(slog.NewJSONHandler(&buf, nil))).With("service", "api")
	_, span := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "s")
	defer span.End()
	ctx := trace.ContextWithSpan(context.Background(), span)

	logger.InfoContext(ctx, "with span")
	logger.Info("without context")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %s", len(lines), buf.String())
	}
	var withSpan, without map[string]any
	if err := json.Unmarshal(lines[0], &withSpan); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := json.Unmarshal(lines[1], &without); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if withSpan["trace_id"] != span.SpanContext().TraceID().String() ||
		withSpan["span_id"] != span.SpanContext().SpanID().String() {
		t.Errorf("record with span = %v, want trace_id %s", withSpan, span.SpanContext().TraceID())
	}
	if withSpan["service"] != "api" {
		t.Errorf("attrs from With were lost: %v", withSpan)
	}
	if _, ok := without["trace_id"]; ok {
		t.Errorf("record without span must not carry trace_id: %v", without)
	}
}

// capture запоминает записи логов, отправленные в OTLP-провайдер.
type capture struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (c *capture) OnEmit(_ context.Context, r *sdklog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}
func (*capture) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (*capture) Shutdown(context.Context) error                         { return nil }
func (*capture) ForceFlush(context.Context) error                       { return nil }

func TestInstallLogBridge(t *testing.T) { //nolint:paralleltest // меняет глобальные slog и провайдер логов
	prevLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	c := &capture{}
	otel.SetLoggerProvider(sdklog.NewLoggerProvider(sdklog.WithProcessor(c)))

	var stdout bytes.Buffer
	slog.SetDefault(slog.New(WithTraceIDs(slog.NewJSONHandler(&stdout, nil))))
	InstallLogBridge("test-service")

	_, span := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "s")
	defer span.End()
	slog.InfoContext(trace.ContextWithSpan(context.Background(), span), "hello")

	if stdout.Len() == 0 {
		t.Error("record did not reach stdout")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.records) != 1 {
		t.Fatalf("OTLP got %d records, want 1", len(c.records))
	}
	got := c.records[0]
	if got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("OTLP record trace = %s/%s, want %s/%s",
			got.TraceID(), got.SpanID(), span.SpanContext().TraceID(), span.SpanContext().SpanID())
	}
	if got.Body().AsString() != "hello" {
		t.Errorf("body = %q, want hello", got.Body().AsString())
	}
}
