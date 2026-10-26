package telemetry

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Setup настраивает трейсы, метрики и логи: все три сигнала уходят по OTLP/gRPC в коллектор.
// Адрес и имя сервиса берутся из OTEL_EXPORTER_OTLP_ENDPOINT и OTEL_SERVICE_NAME.
// Возвращённая функция сбрасывает буферы и останавливает провайдеры.
//
// Логи только регистрируются в глобальном провайдере; к slog их подключает InstallLogBridge, чтобы
// порядок инициализации был виден в одном месте (app.SetupTelemetry).
func Setup(ctx context.Context) (func(context.Context) error, error) {
	res, err := resource.New(ctx, resource.WithFromEnv())
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	var shutdowns []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		var errs []error
		for _, fn := range slices.Backward(shutdowns) {
			errs = append(errs, fn(ctx))
		}
		return errors.Join(errs...)
	}
	// fail останавливает уже созданные провайдеры и возвращает исходную ошибку.
	fail := func(err error) (func(context.Context) error, error) {
		return nil, errors.Join(err, shutdown(ctx))
	}

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return fail(fmt.Errorf("create otlp trace exporter: %w", err))
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	shutdowns = append(shutdowns, tracerProvider.Shutdown)

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return fail(fmt.Errorf("create otlp metric exporter: %w", err))
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(durationBucketsView()),
	)
	shutdowns = append(shutdowns, meterProvider.Shutdown)

	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return fail(fmt.Errorf("create otlp log exporter: %w", err))
	}
	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)
	shutdowns = append(shutdowns, loggerProvider.Shutdown)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetLoggerProvider(loggerProvider)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))

	// Метрики рантайма Go: горутины, куча, GC. Читаются при каждом экспорте.
	if err := runtime.Start(runtime.WithMeterProvider(meterProvider)); err != nil {
		return fail(fmt.Errorf("start runtime metrics: %w", err))
	}

	return shutdown, nil
}

// InjectHeaders кладёт trace-контекст из ctx в заголовки записи outbox и возвращает их. Relay переносит
// заголовки в Kafka как есть, поэтому цепочка трейса не рвётся на пути БД → relay → следующий консьюмер.
// Переданная мапа изменяется на месте; при nil создаётся новая, поэтому используйте результат.
// Без активного span'а (или без настроенного провайдера) заголовки не меняются.
func InjectHeaders(ctx context.Context, headers map[string]string) map[string]string {
	if headers == nil {
		headers = make(map[string]string, 1)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	return headers
}

// RecordError помечает span ошибочным и сохраняет ошибку в нём. Для nil ничего не делает,
// поэтому его можно вызывать на любом выходе без проверки.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// durationBuckets — границы гистограмм длительности в секундах. Корзины по умолчанию начинаются
// с 5 мс, а типичный RPC и транзакция укладываются в 1–3 мс, то есть целиком в первую корзину,
// и квантили на дашборде теряют смысл.
var durationBuckets = []float64{
	0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// durationBucketsView применяет durationBuckets ко всем гистограммам в секундах, включая
// rpc.server.call.duration из otelgrpc.
func durationBucketsView() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: durationBuckets}},
	)
}
