package telemetry

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRecordError(t *testing.T) {
	t.Parallel()

	sr := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)).Tracer("test")

	_, ok := tracer.Start(context.Background(), "ok")
	RecordError(ok, nil)
	ok.End()

	_, failed := tracer.Start(context.Background(), "failed")
	RecordError(failed, errors.New("boom"))
	failed.End()

	for _, s := range sr.Ended() {
		wantErr := s.Name() == "failed"
		if gotErr := s.Status().Code == codes.Error; gotErr != wantErr {
			t.Errorf("span %q: error status = %v, want %v", s.Name(), gotErr, wantErr)
		}
		if gotEvents := len(s.Events()); (gotEvents > 0) != wantErr {
			t.Errorf("span %q: %d events, want error event = %v", s.Name(), gotEvents, wantErr)
		}
	}
}

func TestInjectHeadersNilMap(t *testing.T) { //nolint:paralleltest // меняет глобальные провайдер и propagator OTel
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx, span := otel.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	if got := InjectHeaders(ctx, nil); got["traceparent"] == "" {
		t.Fatalf("nil map: headers = %v, want traceparent", got)
	}

	own := map[string]string{"type": "X"}
	if got := InjectHeaders(ctx, own); got["type"] != "X" || got["traceparent"] == "" {
		t.Fatalf("existing map: headers = %v, want type and traceparent", got)
	}

	if got := InjectHeaders(context.Background(), nil); len(got) != 0 {
		t.Fatalf("no active span: headers = %v, want empty", got)
	}
}

func TestDurationBucketsView(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(durationBucketsView()))
	meter := mp.Meter("test")
	seconds, _ := meter.Float64Histogram("d", metric.WithUnit("s"))
	count, _ := meter.Int64Histogram("n", metric.WithUnit("{record}"))
	seconds.Record(t.Context(), 0.002)
	count.Record(t.Context(), 3)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, m := range rm.ScopeMetrics[0].Metrics {
		switch data := m.Data.(type) {
		case metricdata.Histogram[float64]:
			if got := data.DataPoints[0].Bounds; len(got) != len(durationBuckets) || got[0] != 0.0005 {
				t.Errorf("%s bounds = %v, want %v", m.Name, got, durationBuckets)
			}
		case metricdata.Histogram[int64]:
			if got := data.DataPoints[0].Bounds; len(got) == len(durationBuckets) {
				t.Errorf("%s (not seconds) must keep default bounds, got %v", m.Name, got)
			}
		}
	}
}
