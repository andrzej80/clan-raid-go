// Package app содержит общий каркас запуска бинарников: конфиг, логи, сигналы, телеметрия.
// Порядок остановки зависимостей (сервер, продюсер, консьюмер, пул) остаётся в самих main.
package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

// telemetryShutdownTimeout — сколько ждём финального сброса метрик и трейсов в коллектор.
const telemetryShutdownTimeout = 5 * time.Second

// Main загружает конфиг, включает JSON-логи и запускает run. Любая ошибка — код выхода 1.
func Main(name string, run func(ctx context.Context, cfg *config.Config) error) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}

	ctx, stop := SignalContext()
	err = run(ctx, cfg)
	stop()
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
	slog.Info("stopped", "service", name)
}

// SignalContext возвращает контекст, отменяемый по SIGINT и SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// SetupTelemetry настраивает трейсы и метрики. Возвращённую функцию нужно вызвать через defer:
// она сбрасывает остатки в коллектор с собственным контекстом, потому что к моменту остановки
// родительский ctx уже отменён сигналом.
func SetupTelemetry(ctx context.Context) (func(), error) {
	shutdown, err := telemetry.Setup(ctx)
	if err != nil {
		return nil, err
	}
	return func() { //nolint:contextcheck // сброс при остановке: родительский ctx уже отменён
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryShutdownTimeout)
		defer cancel()
		if err := shutdown(flushCtx); err != nil {
			slog.Warn("telemetry shutdown", "err", err)
		}
	}, nil
}
