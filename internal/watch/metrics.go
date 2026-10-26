package watch

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const meterName = "github.com/andrzej80/clan-raid-go/internal/watch"

type hubMetrics struct {
	slowDisconnects metric.Int64Counter
}

// newHubMetrics регистрирует метрики hub.
//
// Число подписчиков и комнат — observable gauge: значение считается из карты рейдов при экспорте,
// а не ведётся счётчиком вверх-вниз, поэтому оно не может разойтись с реальностью при остановке hub
// или повторной отписке. Подсчёт идёт под RLock и стоит O(число рейдов) раз в период экспорта.
func newHubMetrics(mp metric.MeterProvider, h *Hub) (*hubMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	slow, err := meter.Int64Counter("raidboss.watch.slow_disconnects",
		metric.WithDescription("WatchRaid subscribers dropped because they did not read their updates"),
		metric.WithUnit("{subscriber}"))
	if err != nil {
		return nil, fmt.Errorf("create slow disconnects counter: %w", err)
	}

	subscribers, err := meter.Int64ObservableGauge("raidboss.watch.subscribers",
		metric.WithDescription("Open WatchRaid subscriptions on this instance"),
		metric.WithUnit("{subscriber}"))
	if err != nil {
		return nil, fmt.Errorf("create subscribers gauge: %w", err)
	}
	rooms, err := meter.Int64ObservableGauge("raidboss.watch.rooms",
		metric.WithDescription("Raids with at least one local subscriber (one source subscription each)"),
		metric.WithUnit("{raid}"))
	if err != nil {
		return nil, fmt.Errorf("create rooms gauge: %w", err)
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		nSubs, nRooms := h.counts()
		o.ObserveInt64(subscribers, nSubs)
		o.ObserveInt64(rooms, nRooms)
		return nil
	}, subscribers, rooms)
	if err != nil {
		return nil, fmt.Errorf("register hub gauges: %w", err)
	}
	return &hubMetrics{slowDisconnects: slow}, nil
}

// noopHubMetrics — запасной вариант, если метрики не удалось зарегистрировать: hub работает без них.
func noopHubMetrics() *hubMetrics {
	return &hubMetrics{slowDisconnects: noop.Int64Counter{}}
}
