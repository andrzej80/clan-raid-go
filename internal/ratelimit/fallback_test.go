package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stubLimiter struct {
	calls   atomic.Int32
	mu      sync.Mutex
	err     error
	block   chan struct{} // если задан, Allow ждёт закрытия
	entered chan struct{} // если задан, Allow сообщает о входе
}

func (s *stubLimiter) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *stubLimiter) Allow(context.Context, int64) (bool, time.Duration, error) {
	s.calls.Add(1)
	if s.entered != nil {
		s.entered <- struct{}{}
	}
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return false, 0, s.err
	}
	return true, 0, nil
}

func newTestFallback(primary, secondary Limiter) (*Fallback, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	return NewFallback(primary, secondary,
		WithFailureThreshold(3), WithCooldown(2*time.Second), WithFallbackClock(clock.Now)), clock
}

func fbAllow(t *testing.T, f *Fallback) (bool, time.Duration) {
	t.Helper()
	ok, retry, err := f.Allow(context.Background(), 1)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return ok, retry
}

func TestFallback_HealthyPrimaryIsUsed(t *testing.T) {
	t.Parallel()
	primary, secondary := &stubLimiter{}, &stubLimiter{}
	f, _ := newTestFallback(primary, secondary)

	for range 5 {
		fbAllow(t, f)
	}
	if primary.calls.Load() != 5 || secondary.calls.Load() != 0 {
		t.Fatalf("primary=%d secondary=%d, want 5/0", primary.calls.Load(), secondary.calls.Load())
	}
}

// Отказ primary по лимиту (ok=false без ошибки) — штатный ответ, а не сбой.
func TestFallback_RateLimitRejectionIsNotFailure(t *testing.T) {
	t.Parallel()
	primary := NewMemory(5, 1)
	secondary := &stubLimiter{}
	f, _ := newTestFallback(primary, secondary)

	for range 10 {
		fbAllow(t, f)
	}
	if secondary.calls.Load() != 0 {
		t.Fatal("secondary must not be used while primary answers")
	}
}

func TestFallback_FailedCallIsServedBySecondary(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: errors.New("redis down")}
	secondary := &stubLimiter{}
	f, _ := newTestFallback(primary, secondary)

	if ok, _ := fbAllow(t, f); !ok {
		t.Fatal("request must be answered by secondary")
	}
	if secondary.calls.Load() != 1 {
		t.Fatalf("secondary calls = %d, want 1", secondary.calls.Load())
	}
}

func TestFallback_BreakerOpensAfterThreshold(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: errors.New("redis down")}
	f, _ := newTestFallback(primary, &stubLimiter{})

	for range 3 {
		fbAllow(t, f)
	}
	for range 10 {
		fbAllow(t, f)
	}
	if got := primary.calls.Load(); got != 3 {
		t.Fatalf("primary calls = %d, want 3: open breaker must skip primary", got)
	}
}

func TestFallback_SuccessResetsFailureCount(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{}
	f, _ := newTestFallback(primary, &stubLimiter{})

	for range 5 { // две ошибки, успех, две ошибки: подряд трёх нет
		primary.setErr(errors.New("blip"))
		fbAllow(t, f)
		fbAllow(t, f)
		primary.setErr(nil)
		fbAllow(t, f)
	}
	if got := primary.calls.Load(); got != 15 {
		t.Fatalf("primary calls = %d, want 15: breaker must stay closed", got)
	}
}

func TestFallback_ProbeAfterCooldownRecovers(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: errors.New("redis down")}
	f, clock := newTestFallback(primary, &stubLimiter{})
	for range 3 {
		fbAllow(t, f)
	}

	clock.Advance(1999 * time.Millisecond)
	fbAllow(t, f)
	if primary.calls.Load() != 3 {
		t.Fatal("primary must not be probed before cooldown ends")
	}

	primary.setErr(nil)
	clock.Advance(time.Millisecond)
	fbAllow(t, f) // пробный запрос
	fbAllow(t, f) // breaker закрыт
	if got := primary.calls.Load(); got != 5 {
		t.Fatalf("primary calls = %d, want 5 (3 failures + probe + one more)", got)
	}
}

func TestFallback_FailedProbeReopensBreaker(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: errors.New("redis down")}
	f, clock := newTestFallback(primary, &stubLimiter{})
	for range 3 {
		fbAllow(t, f)
	}

	clock.Advance(2 * time.Second)
	fbAllow(t, f) // пробный запрос провалился
	fbAllow(t, f)
	fbAllow(t, f)
	if got := primary.calls.Load(); got != 4 {
		t.Fatalf("primary calls = %d, want 4: a failed probe must reopen the breaker", got)
	}

	clock.Advance(2 * time.Second)
	fbAllow(t, f)
	if got := primary.calls.Load(); got != 5 {
		t.Fatalf("primary calls = %d, want 5: next probe after a new cooldown", got)
	}
}

func TestFallback_OnlyOneConcurrentProbe(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: errors.New("redis down")}
	f, clock := newTestFallback(primary, &stubLimiter{})
	for range 3 {
		fbAllow(t, f)
	}

	primary.block = make(chan struct{})
	primary.entered = make(chan struct{})
	clock.Advance(2 * time.Second)

	var wg sync.WaitGroup
	wg.Go(func() { _, _, _ = f.Allow(context.Background(), 1) })
	<-primary.entered // пробный запрос застрял внутри primary

	for range 20 { // пока он идёт, остальные обслуживает secondary
		fbAllow(t, f)
	}
	close(primary.block)
	wg.Wait()

	if got := primary.calls.Load(); got != 4 {
		t.Fatalf("primary calls = %d, want 4 (3 failures + exactly one probe)", got)
	}
}

// Отмена запроса клиентом — не вина primary: breaker не открывается.
func TestFallback_CancelledRequestDoesNotOpenBreaker(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: context.Canceled}
	f, _ := newTestFallback(primary, &stubLimiter{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 10 {
		if _, _, err := f.Allow(ctx, 1); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}
	if got := primary.calls.Load(); got != 10 {
		t.Fatalf("primary calls = %d, want 10: cancelled requests must not trip the breaker", got)
	}
}

// Redis недоступен, лимит держит Memory и действует правило 4: burst 10, потом отказ с задержкой 200 мс.
func TestFallback_MemoryEnforcesRuleWhenRedisIsDown(t *testing.T) {
	t.Parallel()
	primary := &stubLimiter{err: errors.New("redis down")}
	secondary, _ := newTestLimiter()
	f, _ := newTestFallback(primary, secondary)

	for i := range 10 {
		if ok, _ := fbAllow(t, f); !ok {
			t.Fatalf("request %d of the burst was rejected", i+1)
		}
	}
	ok, retry := fbAllow(t, f)
	if ok || ceilMs(retry) != 200*time.Millisecond {
		t.Fatalf("ok=%v retry=%v, want rejected with 200ms", ok, retry)
	}
}

// Когда Redis возвращается, лимит снова общий.
func TestFallback_RealRedisRecovery(t *testing.T) {
	t.Parallel()
	r, _ := newRedisLimiter(t)
	flaky := &flakyLimiter{inner: r}
	flaky.down.Store(true)
	f, clock := newTestFallback(flaky, NewMemory(5, 10))
	player := newPlayerID()

	for range 5 {
		if _, _, err := f.Allow(context.Background(), player); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}
	flaky.down.Store(false)
	clock.Advance(2 * time.Second)

	before := flaky.calls.Load()
	if _, _, err := f.Allow(context.Background(), player); err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if flaky.calls.Load() != before+1 {
		t.Fatal("primary must be probed after cooldown")
	}
}

type flakyLimiter struct {
	inner Limiter
	down  atomic.Bool
	calls atomic.Int32
}

func (l *flakyLimiter) Allow(ctx context.Context, playerID int64) (bool, time.Duration, error) {
	l.calls.Add(1)
	if l.down.Load() {
		return false, 0, errors.New("redis down")
	}
	return l.inner.Allow(ctx, playerID)
}
