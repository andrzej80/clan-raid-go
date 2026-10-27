package raid

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/andrzej80/clan-raid-go/internal/breaker"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const guardMeterName = "github.com/andrzej80/clan-raid-go/internal/raid"

// Проверка живости очереди: пока предохранитель разомкнут, раз в healthInterval вызывается Ping.
// Если он прошёл, пропускается малая доля ударов (breaker.Config.ProbeRatio), и по их успеху
// предохранитель замыкается. Реальные удары игроков пробой не служат, пока очередь не отвечает.
const (
	healthInterval = 2 * time.Second
	healthTimeout  = time.Second
)

// QueueHealth проверяет доступность очереди (kafka.Producer.Ping).
type QueueHealth interface {
	Ping(ctx context.Context) error
}

// GuardedPublisher оборачивает HitPublisher предохранителем: пока очередь недоступна, Attack
// сразу получает ErrQueueUnavailable (UNAVAILABLE) и не держит запрос, горутину и соединение
// до таймаута. Раз в OpenFor пропускается один пробный удар, и по его успеху вызовы возобновляются.
type GuardedPublisher struct {
	next     HitPublisher
	health   QueueHealth
	breaker  *breaker.Breaker
	rejected metric.Int64Counter
}

// NewGuardedPublisher создаёт обёртку. cfg.OnStateChange дополняется логом и метрикой переходов
// (ctx нужен только для записи метрики).
func NewGuardedPublisher(
	ctx context.Context, next HitPublisher, health QueueHealth, cfg breaker.Config,
) (*GuardedPublisher, error) {
	meter := otel.GetMeterProvider().Meter(guardMeterName)
	rejected, err := meter.Int64Counter("raidboss.attack.breaker.rejected",
		metric.WithDescription("Attacks rejected immediately because the hit queue circuit breaker is open"),
		metric.WithUnit("{attack}"))
	if err != nil {
		return nil, err
	}
	transitions, err := meter.Int64Counter("raidboss.attack.breaker.transitions",
		metric.WithDescription("Circuit breaker state changes of the hit queue"),
		metric.WithUnit("{transition}"))
	if err != nil {
		return nil, err
	}

	userHook := cfg.OnStateChange
	cfg.OnStateChange = func(from, to breaker.State) {
		slog.Warn("hit queue circuit breaker", "from", from.String(), "to", to.String())
		transitions.Add(ctx, 1, metric.WithAttributes(attribute.String("state", to.String())))
		if userHook != nil {
			userHook(from, to)
		}
	}
	return &GuardedPublisher{next: next, health: health, breaker: breaker.New(cfg), rejected: rejected}, nil
}

// Publish отклоняет удар сразу, если предохранитель разомкнут. Сбоем очереди считается всё, кроме
// отмены и дедлайна клиента: клиент с маленьким дедлайном не должен размыкать предохранитель.
func (g *GuardedPublisher) Publish(ctx context.Context, hit domain.Hit) error {
	err := g.breaker.Do(func() error { return g.next.Publish(ctx, hit) }, isQueueFailure)
	if errors.Is(err, breaker.ErrOpen) {
		g.rejected.Add(ctx, 1)
		return domain.ErrQueueUnavailable
	}
	return err
}

func isQueueFailure(err error) bool {
	return !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)
}

// Run следит за очередью, пока предохранитель разомкнут, и блокируется до отмены ctx.
func (g *GuardedPublisher) Run(ctx context.Context) {
	g.breaker.Run(ctx, healthInterval, healthTimeout, g.health.Ping)
}
