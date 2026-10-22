package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func newClock() *fakeClock { return &fakeClock{now: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)} }

func TestTTL_CachesWithinTTLAndReloadsAfter(t *testing.T) {
	t.Parallel()

	clock := newClock()
	var loads atomic.Int32
	c := NewTTL(time.Second, func(_ context.Context, key int) (int, error) {
		return key*10 + int(loads.Add(1)), nil
	}, WithClock(clock.Now))
	ctx := context.Background()

	first, err := c.Get(ctx, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	clock.Advance(999 * time.Millisecond)
	second, _ := c.Get(ctx, 1)
	if first != second || loads.Load() != 1 {
		t.Fatalf("within TTL: first=%d second=%d loads=%d, want same value and 1 load", first, second, loads.Load())
	}

	clock.Advance(2 * time.Millisecond) // TTL истёк
	third, _ := c.Get(ctx, 1)
	if third == first || loads.Load() != 2 {
		t.Fatalf("after TTL: third=%d loads=%d, want reload", third, loads.Load())
	}
}

func TestTTL_KeysAreIndependent(t *testing.T) {
	t.Parallel()

	var loads atomic.Int32
	c := NewTTL(time.Minute, func(_ context.Context, key int) (int, error) {
		loads.Add(1)
		return key, nil
	})
	for _, k := range []int{1, 2, 1, 2, 3} {
		if v, _ := c.Get(context.Background(), k); v != k {
			t.Fatalf("Get(%d) = %d", k, v)
		}
	}
	if loads.Load() != 3 {
		t.Fatalf("loads = %d, want 3 (one per key)", loads.Load())
	}
}

func TestTTL_ErrorsAreNotCached(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	var loads atomic.Int32
	c := NewTTL(time.Minute, func(context.Context, int) (int, error) {
		if loads.Add(1) == 1 {
			return 0, boom
		}
		return 42, nil
	})
	ctx := context.Background()

	if _, err := c.Get(ctx, 1); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	v, err := c.Get(ctx, 1)
	if err != nil || v != 42 {
		t.Fatalf("after error: v=%d err=%v, want 42/nil (error must not be cached)", v, err)
	}
}

// Критерий этапа 2: при промахе кэша N одновременных запросов делают одну загрузку.
func TestTTL_ConcurrentColdMissLoadsOnce(t *testing.T) {
	t.Parallel()

	const callers = 1000
	var loads atomic.Int32
	c := NewTTL(time.Minute, func(context.Context, int) (int, error) {
		loads.Add(1)
		<-time.After(20 * time.Millisecond) // держим загрузку открытой, пока все придут
		return 7, nil
	})

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range callers {
		wg.Go(func() {
			<-start
			if v, err := c.Get(context.Background(), 1); err != nil || v != 7 {
				t.Errorf("Get: v=%d err=%v", v, err)
			}
		})
	}
	close(start)
	wg.Wait()

	if loads.Load() != 1 {
		t.Fatalf("loads = %d, want 1", loads.Load())
	}
}

func BenchmarkTTL_ConcurrentColdMiss(b *testing.B) {
	const callers = 1000
	var loads atomic.Int64

	for range b.N {
		c := NewTTL(time.Minute, func(context.Context, int) (int, error) {
			loads.Add(1)
			<-time.After(5 * time.Millisecond)
			return 7, nil
		})
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() { _, _ = c.Get(context.Background(), 1) })
		}
		wg.Wait()
	}
	b.ReportMetric(float64(loads.Load())/float64(b.N), "loads/op")
}

// Вызывающий выходит по своему дедлайну, а загрузка не прерывается и результат кэшируется.
func TestTTL_CallerDeadlineDoesNotAbortLoad(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	var loads atomic.Int32
	c := NewTTL(time.Minute, func(context.Context, int) (int, error) {
		loads.Add(1)
		<-release
		return 5, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}

	close(release)
	// Ожидающий с нормальным контекстом получает результат той же загрузки.
	v, err := c.Get(context.Background(), 1)
	if err != nil || v != 5 {
		t.Fatalf("v=%d err=%v, want 5/nil", v, err)
	}
	if loads.Load() != 1 {
		t.Fatalf("loads = %d, want 1", loads.Load())
	}
}

func TestTTL_CancelledInitiatorDoesNotFailOthers(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	c := NewTTL(time.Minute, func(ctx context.Context, _ int) (int, error) {
		close(started)
		select {
		case <-release:
			return 9, nil
		case <-ctx.Done():
			return 0, ctx.Err() // загрузка не должна отменяться вместе с инициатором
		}
	})

	initCtx, cancelInit := context.WithCancel(context.Background())
	go func() { _, _ = c.Get(initCtx, 1) }()
	<-started

	got := make(chan error, 1)
	go func() {
		v, err := c.Get(context.Background(), 1)
		if err == nil && v != 9 {
			err = errors.New("wrong value")
		}
		got <- err
	}()

	cancelInit()
	<-time.After(20 * time.Millisecond)
	close(release)

	if err := <-got; err != nil {
		t.Fatalf("second caller failed after initiator cancel: %v", err)
	}
}

func TestTTL_SweepsExpiredEntries(t *testing.T) {
	t.Parallel()

	clock := newClock()
	c := NewTTL(time.Second, func(_ context.Context, key int) (int, error) { return key, nil }, WithClock(clock.Now))
	ctx := context.Background()

	for k := range minSweepSize {
		_, _ = c.Get(ctx, k)
	}
	clock.Advance(time.Minute) // всё протухло
	for k := minSweepSize; k < minSweepSize+10; k++ {
		_, _ = c.Get(ctx, k)
	}

	c.mu.Lock()
	size := len(c.items)
	c.mu.Unlock()
	if size > 20 {
		t.Fatalf("map holds %d entries, expired ones must be swept", size)
	}
}
