package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/trace"
)

// WithTraceIDs оборачивает handler: в каждую запись, сделанную с контекстом активного span'а
// (slog.InfoContext и т.п.), добавляются trace_id и span_id. Нужно для stdout: в OTLP trace-контекст
// уходит отдельными полями записи, и мост otelslog сам его берёт из контекста.
//
// Записи без контекста (slog.Info) идентификаторов не получают: у них нет span'а.
// Атрибуты добавляются в конец записи, поэтому внутри группы (WithGroup) они окажутся вложенными.
func WithTraceIDs(h slog.Handler) slog.Handler { return traceHandler{h} }

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// InstallLogBridge дополняет логгер по умолчанию отправкой записей по OTLP: прежний handler (stdout)
// остаётся, рядом появляется мост otelslog с глобальным провайдером логов (его задаёт Setup).
// Вызывать после Setup. В OTLP запись уходит вместе с trace_id и span_id из контекста.
func InstallLogBridge(service string) {
	slog.SetDefault(slog.New(slog.NewMultiHandler(
		slog.Default().Handler(),
		otelslog.NewHandler(service),
	)))
}
