package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	fallbackMeterName       = "github.com/andrzej80/clan-raid-go/internal/ratelimit"
	defaultFailureThreshold = 3
	defaultBreakerCooldown  = 2 * time.Second
)

// Limiter решает, можно ли игроку выполнить ещё один запрос (тот же контракт, что у interceptors.Limiter).
type Limiter interface {
	Allow(ctx context.Context, playerID int64) (ok bool, retryAfter time.Duration, err error)
}

// Fallback ограничивает частоту через primary (Redis, общий лимит кластера), а при его сбоях
// переключается на secondary (Memory, лимит одного экземпляра): так при падении Redis лимит
// ослабляется до «N экземпляров × норма», но не пропадает совсем.
//
// Чтобы упавший Redis не добавлял таймаут к каждому запросу, работает circuit breaker: после
// threshold ошибок подряд primary не вызывается cooldown, затем пускается один пробный запрос.
type Fallback struct {
	primary, secondary Limiter
	threshold          int
	cooldown           time.Duration
	now                func() time.Time
	fallbacks          metric.Int64Counter

	mu        sync.Mutex
	failures  int
	openUntil time.Time
	probing   bool
}

type FallbackOption func(*Fallback)

// WithFailureThreshold: сколько ошибок primary подряд открывают breaker.
func WithFailureThreshold(n int) FallbackOption {
	return func(f *Fallback) { f.threshold = n }
}

// WithCooldown: как долго primary не вызывается после открытия breaker.
func WithCooldown(d time.Duration) FallbackOption {
	return func(f *Fallback) { f.cooldown = d }
}

// WithFallbackClock подменяет источник времени (для тестов).
func WithFallbackClock(now func() time.Time) FallbackOption {
	return func(f *Fallback) { f.now = now }
}

// WithFallbackMeterProvider подменяет провайдер метрик (для тестов).
func WithFallbackMeterProvider(mp metric.MeterProvider) FallbackOption {
	return func(f *Fallback) { f.fallbacks = counterFrom(mp) }
}

func NewFallback(primary, secondary Limiter, opts ...FallbackOption) *Fallback {
	f := &Fallback{
		primary:   primary,
		secondary: secondary,
		threshold: defaultFailureThreshold,
		cooldown:  defaultBreakerCooldown,
		now:       time.Now,
		fallbacks: counterFrom(otel.GetMeterProvider()),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func counterFrom(mp metric.MeterProvider) metric.Int64Counter {
	c, err := mp.Meter(fallbackMeterName).Int64Counter("raidboss.ratelimit.fallback",
		metric.WithDescription("Requests limited by the in-memory fallback because the primary limiter failed"),
		metric.WithUnit("{request}"))
	if err != nil {
		// Ошибка возможна только при некорректном имени инструмента; метрика не стоит остановки сервиса.
		slog.Warn("create ratelimit fallback counter", "err", err)
		return noop.Int64Counter{}
	}
	return c
}

func (f *Fallback) Allow(ctx context.Context, playerID int64) (bool, time.Duration, error) {
	if f.acquirePrimary() {
		ok, retry, err := f.primary.Allow(ctx, playerID)
		if err == nil {
			f.recordSuccess()
			return ok, retry, nil
		}
		f.recordFailure(ctx, err)
	}
	f.fallbacks.Add(ctx, 1)
	return f.secondary.Allow(ctx, playerID)
}

// acquirePrimary решает, можно ли обращаться к primary: breaker закрыт либо пришло время
// единственного пробного запроса.
func (f *Fallback) acquirePrimary() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failures < f.threshold {
		return true
	}
	if f.now().Before(f.openUntil) || f.probing {
		return false
	}
	f.probing = true
	return true
}

func (f *Fallback) recordSuccess() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failures >= f.threshold {
		slog.Info("rate limiter primary recovered, shared limit is back")
	}
	f.failures = 0
	f.probing = false
}

func (f *Fallback) recordFailure(ctx context.Context, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.probing = false
	// Клиент отменил запрос или истёк его дедлайн: Redis тут ни при чём, breaker не трогаем.
	if ctx.Err() != nil {
		return
	}
	f.failures++
	if f.failures >= f.threshold {
		f.openUntil = f.now().Add(f.cooldown)
		if f.failures == f.threshold {
			slog.WarnContext(ctx, "rate limiter primary failed, using in-memory fallback",
				"err", err, "failures", f.failures, "cooldown", f.cooldown)
		}
	}
}

var (
	_ Limiter = (*Fallback)(nil)
	_ Limiter = (*Memory)(nil)
	_ Limiter = (*Redis)(nil)
)
