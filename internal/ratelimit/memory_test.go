package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestLimiter() (*Memory, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)}
	return NewMemory(5, 10, WithClock(clock.Now)), clock
}

func allow(t *testing.T, m *Memory, player int64) (bool, time.Duration) {
	t.Helper()
	ok, retry, err := m.Allow(context.Background(), player)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return ok, retry
}

// Правило 4 из README: 5 ударов в секунду, burst 10.
func TestMemory_BurstThenRate(t *testing.T) {
	t.Parallel()
	m, clock := newTestLimiter()

	for i := range 10 {
		if ok, _ := allow(t, m, 1); !ok {
			t.Fatalf("request %d of the burst was rejected", i+1)
		}
	}
	ok, retry := allow(t, m, 1)
	if ok {
		t.Fatal("11th request in the same instant must be rejected")
	}
	if retry != 200*time.Millisecond {
		t.Fatalf("retry after = %v, want 200ms (1/5 s)", retry)
	}

	clock.Advance(time.Second) // за секунду накапало 5 токенов
	for i := range 5 {
		if ok, _ := allow(t, m, 1); !ok {
			t.Fatalf("request %d after refill was rejected", i+1)
		}
	}
	if ok, _ := allow(t, m, 1); ok {
		t.Fatal("6th request after one second must be rejected")
	}
}

func TestMemory_RetryAfterShrinksWithTime(t *testing.T) {
	t.Parallel()
	m, clock := newTestLimiter()
	for range 10 {
		allow(t, m, 1)
	}

	clock.Advance(50 * time.Millisecond)
	ok, retry := allow(t, m, 1)
	if ok || retry != 150*time.Millisecond {
		t.Fatalf("ok=%v retry=%v, want rejected with 150ms", ok, retry)
	}

	clock.Advance(150 * time.Millisecond)
	if ok, _ := allow(t, m, 1); !ok {
		t.Fatal("request after the advised delay must be allowed")
	}
}

// Отказ не списывает токен: иначе настойчивый клиент никогда бы не дождался.
func TestMemory_RejectionDoesNotConsumeTokens(t *testing.T) {
	t.Parallel()
	m, clock := newTestLimiter()
	for range 10 {
		allow(t, m, 1)
	}
	for range 100 {
		allow(t, m, 1) // отказы
	}

	clock.Advance(200 * time.Millisecond)
	if ok, _ := allow(t, m, 1); !ok {
		t.Fatal("after one token interval a request must pass despite earlier rejections")
	}
}

func TestMemory_PlayersAreIndependent(t *testing.T) {
	t.Parallel()
	m, _ := newTestLimiter()
	for range 10 {
		allow(t, m, 1)
	}

	if ok, _ := allow(t, m, 1); ok {
		t.Fatal("player 1 must be limited")
	}
	if ok, _ := allow(t, m, 2); !ok {
		t.Fatal("player 2 must not be affected by player 1")
	}
}

func TestMemory_SweepsIdlePlayers(t *testing.T) {
	t.Parallel()
	m, clock := newTestLimiter()

	for id := range int64(minSweepSize) {
		allow(t, m, id+1)
	}
	clock.Advance(time.Minute) // все давно простаивают
	for id := range int64(10) {
		allow(t, m, int64(minSweepSize)+id+1)
	}

	m.mu.Lock()
	size := len(m.entries)
	m.mu.Unlock()
	if size > 20 {
		t.Fatalf("limiter keeps %d players, idle ones must be swept", size)
	}
}

func TestMemory_ConcurrentUse(t *testing.T) {
	t.Parallel()
	m := NewMemory(1000, 1000)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			for range 100 {
				if _, _, err := m.Allow(context.Background(), int64(i%5+1)); err != nil {
					t.Errorf("Allow: %v", err)
				}
			}
		})
	}
	wg.Wait()
}
