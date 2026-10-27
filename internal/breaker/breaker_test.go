package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func fail() error { return errBoom }
func ok() error   { return nil }

func isNotClientErr(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// newTest: Rand возвращает 0, поэтому в half-open пропускается каждый вызов (0 < ProbeRatio).
func newTest(threshold int) *Breaker {
	return New(Config{FailureThreshold: threshold, CloseAfter: 3, MaxInconclusive: 2, Rand: func() float64 { return 0 }})
}

// open размыкает предохранитель серией сбоев.
func open(t *testing.T, b *Breaker) {
	t.Helper()
	for range b.cfg.FailureThreshold {
		_ = b.Do(fail, nil)
	}
	if b.State() != Open {
		t.Fatalf("state = %v, want open", b.State())
	}
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()

	b := newTest(3)
	for range 3 {
		if err := b.Do(fail, nil); !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want the dependency error", err)
		}
	}
	if b.State() != Open {
		t.Fatalf("state = %v, want open", b.State())
	}

	var called bool
	err := b.Do(func() error { called = true; return nil }, nil)
	if !errors.Is(err, ErrOpen) || called {
		t.Fatalf("err = %v called = %v: an open breaker must reject without calling", err, called)
	}
}

func TestSuccessResetsFailureStreak(t *testing.T) {
	t.Parallel()

	b := newTest(3)
	_ = b.Do(fail, nil)
	_ = b.Do(fail, nil)
	_ = b.Do(ok, nil)
	_ = b.Do(fail, nil)
	_ = b.Do(fail, nil)
	if b.State() != Closed {
		t.Fatalf("state = %v: failures must be consecutive to open the breaker", b.State())
	}
}

func TestOpenStaysOpenWithoutHealthCheck(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	open(t, b)
	for range 100 {
		if err := b.Do(ok, nil); !errors.Is(err, ErrOpen) {
			t.Fatalf("err = %v: only the health check may leave the open state", err)
		}
	}
}

func TestHalfOpenAdmitsOnlyProbeRatio(t *testing.T) {
	t.Parallel()

	rolls := []float64{0.01, 0.5, 0.049, 0.99}
	var i int
	b := New(Config{FailureThreshold: 1, ProbeRatio: 0.05, CloseAfter: 100, Rand: func() float64 {
		defer func() { i++ }()
		return rolls[i%len(rolls)]
	}})
	open(t, b)
	i = 0 // open() не вызывает Rand в Closed/Open, но так тест не зависит от этого
	if !b.HalfOpen() {
		t.Fatal("HalfOpen must succeed on an open breaker")
	}

	admitted := make([]bool, 0, len(rolls))
	for range rolls {
		err := b.Do(ok, nil)
		admitted = append(admitted, err == nil)
	}
	want := []bool{true, false, true, false}
	for k := range want {
		if admitted[k] != want[k] {
			t.Fatalf("admitted = %v, want %v", admitted, want)
		}
	}
}

func TestHalfOpenClosesAfterEnoughSuccesses(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	open(t, b)
	b.HalfOpen()

	for range 2 {
		_ = b.Do(ok, nil)
		if b.State() != HalfOpen {
			t.Fatalf("state = %v: a single success must not close the breaker", b.State())
		}
	}
	_ = b.Do(ok, nil)
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed after CloseAfter successes", b.State())
	}
}

func TestHalfOpenReopensOnFailure(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	open(t, b)
	b.HalfOpen()

	_ = b.Do(ok, nil)
	_ = b.Do(fail, nil)
	if b.State() != Open {
		t.Fatalf("state = %v, want open after a failed probe", b.State())
	}
	if err := b.Do(ok, nil); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v: after a failed probe only the health check may reopen traffic", err)
	}
}

func TestInconclusiveProbesAreBounded(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	open(t, b)
	b.HalfOpen()

	canceled := func() error { return context.Canceled }
	_ = b.Do(canceled, isNotClientErr)
	if b.State() != HalfOpen {
		t.Fatalf("state = %v, want half-open after one inconclusive probe", b.State())
	}
	_ = b.Do(canceled, isNotClientErr)
	if b.State() != Open {
		t.Fatalf("state = %v, want open: probes without a verdict must not keep half-open forever", b.State())
	}
}

func TestClientErrorsAreNotFailures(t *testing.T) {
	t.Parallel()

	b := newTest(2)
	for range 5 {
		_ = b.Do(func() error { return context.Canceled }, isNotClientErr)
	}
	if b.State() != Closed {
		t.Fatalf("state = %v: client cancellations must not open the breaker", b.State())
	}
}

func TestStaleResultsAreIgnored(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	open(t, b)
	b.HalfOpen()

	// Долгий пробный вызов начался в этом half-open, а закончится после того, как состояние сменилось.
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Do(func() error { close(started); <-release; return errBoom }, nil)
	}()
	<-started

	// Остальные пробы замыкают предохранитель.
	for range 3 {
		_ = b.Do(ok, nil)
	}
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed", b.State())
	}
	close(release)
	<-done
	if b.State() != Closed {
		t.Fatalf("state = %v: a failure from a previous epoch must not reopen the breaker", b.State())
	}
}

func TestHalfOpenOnlyFromOpen(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	if b.HalfOpen() {
		t.Fatal("HalfOpen must be a no-op on a closed breaker")
	}
}

func TestRunMovesToHalfOpenWhenProbeSucceeds(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	open(t, b)

	var probes atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(ctx, 5*time.Millisecond, time.Second, func(context.Context) error {
			if probes.Add(1) < 3 {
				return errBoom // зависимость ещё лежит
			}
			return nil
		})
	}()

	deadline := time.After(5 * time.Second)
	for b.State() != HalfOpen {
		select {
		case <-deadline:
			t.Fatalf("state = %v after %d probes, want half-open", b.State(), probes.Load())
		case <-time.After(time.Millisecond):
		}
	}
	if got := probes.Load(); got != 3 {
		t.Fatalf("probes = %d, want 3: failed probes must keep the breaker open", got)
	}
	cancel()
	<-done
}

func TestRunDoesNotProbeWhenClosed(t *testing.T) {
	t.Parallel()

	b := newTest(1)
	var probes atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	b.Run(ctx, 5*time.Millisecond, time.Second, func(context.Context) error {
		probes.Add(1)
		return nil
	})
	if probes.Load() != 0 {
		t.Fatalf("probes = %d: a closed breaker needs no health check", probes.Load())
	}
}

func TestStateChangeCallback(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var transitions []string
	b := New(Config{
		FailureThreshold: 1, CloseAfter: 1, Rand: func() float64 { return 0 },
		OnStateChange: func(from, to State) {
			mu.Lock()
			transitions = append(transitions, from.String()+">"+to.String())
			mu.Unlock()
		},
	})

	_ = b.Do(fail, nil)
	b.HalfOpen()
	_ = b.Do(ok, nil)

	want := []string{"closed>open", "open>half-open", "half-open>closed"}
	mu.Lock()
	defer mu.Unlock()
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", transitions, want)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	t.Parallel()

	b := New(Config{FailureThreshold: 10})
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 100 {
				if errors.Is(b.Do(fail, nil), ErrOpen) {
					rejected.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if rejected.Load() == 0 || b.State() != Open {
		t.Fatalf("rejected = %d state = %v: the breaker must open under load", rejected.Load(), b.State())
	}
}

// При малом RPS доля трафика почти не даёт проб: не реже одного вызова в MinProbeInterval пропускается всегда.
func TestHalfOpenGuaranteesMinimumProbeRate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	b := New(Config{
		FailureThreshold: 1, CloseAfter: 100, MinProbeInterval: 500 * time.Millisecond,
		Rand: func() float64 { return 0.99 }, // по доле трафика не проходит никто
		Now:  func() time.Time { return now },
	})
	open(t, b)
	b.HalfOpen()

	if err := b.Do(ok, nil); err != nil {
		t.Fatalf("first call err = %v: the first call after half-open must be a probe", err)
	}
	if err := b.Do(ok, nil); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v: calls within MinProbeInterval must be rejected", err)
	}
	now = now.Add(500 * time.Millisecond)
	if err := b.Do(ok, nil); err != nil {
		t.Fatalf("err = %v: a probe is due after MinProbeInterval", err)
	}
}

// При малой нагрузке CloseAfter не набрать: одного успеха и SettleTime без сбоев достаточно.
func TestHalfOpenClosesAfterSettleTimeWithOneSuccess(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	b := New(Config{
		FailureThreshold: 1, CloseAfter: 100, SettleTime: time.Second,
		Rand: func() float64 { return 0 }, Now: func() time.Time { return now },
	})
	open(t, b)
	b.HalfOpen()

	if err := b.Do(ok, nil); err != nil {
		t.Fatalf("probe err = %v", err)
	}
	now = now.Add(500 * time.Millisecond)
	_ = b.Do(ok, nil)
	if b.State() != HalfOpen {
		t.Fatalf("state = %v: SettleTime has not passed yet", b.State())
	}

	now = now.Add(500 * time.Millisecond)
	if err := b.Do(ok, nil); err != nil {
		t.Fatalf("err = %v", err)
	}
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed after one success and SettleTime without failures", b.State())
	}
}

func TestHalfOpenDoesNotSettleWithoutSuccess(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	b := New(Config{
		FailureThreshold: 1, CloseAfter: 100, SettleTime: time.Second,
		Rand: func() float64 { return 0 }, Now: func() time.Time { return now },
	})
	open(t, b)
	b.HalfOpen()

	now = now.Add(10 * time.Second)
	_ = b.Do(func() error { return context.Canceled }, isNotClientErr)
	if b.State() != HalfOpen {
		t.Fatalf("state = %v: silence is not evidence that the dependency is healthy", b.State())
	}
}
