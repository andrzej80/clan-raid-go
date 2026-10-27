package raid

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/breaker"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

type countingPublisher struct {
	err   error
	calls atomic.Int32
}

func (p *countingPublisher) Publish(context.Context, domain.Hit) error {
	p.calls.Add(1)
	return p.err
}

type stubHealth struct{ err error }

func (h stubHealth) Ping(context.Context) error { return h.err }

// В half-open пропускается каждый вызов (Rand = 0), чтобы тест не зависел от случая.
func newGuarded(t *testing.T, next HitPublisher) *GuardedPublisher {
	t.Helper()
	g, err := NewGuardedPublisher(context.Background(), next, stubHealth{}, breaker.Config{
		FailureThreshold: 2, CloseAfter: 2, SettleTime: time.Second, Rand: func() float64 { return 0 }, Now: steppingClock(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGuardedPublisherRejectsFastWhenOpen(t *testing.T) {
	t.Parallel()

	next := &countingPublisher{err: errors.New("broker down")}
	g := newGuarded(t, next)
	hit := domain.Hit{ID: uuid.New()}

	for range 2 {
		if err := g.Publish(context.Background(), hit); err == nil {
			t.Fatal("expected the broker error")
		}
	}
	err := g.Publish(context.Background(), hit)
	if !errors.Is(err, domain.ErrQueueUnavailable) {
		t.Fatalf("err = %v, want ErrQueueUnavailable", err)
	}
	if got := next.calls.Load(); got != 2 {
		t.Fatalf("calls = %d: the open breaker must not reach the queue", got)
	}
}

func TestGuardedPublisherIgnoresClientDeadlines(t *testing.T) {
	t.Parallel()

	next := &countingPublisher{err: context.DeadlineExceeded}
	g := newGuarded(t, next)

	for range 10 {
		if err := g.Publish(context.Background(), domain.Hit{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the client deadline passed through", err)
		}
	}
	if got := next.calls.Load(); got != 10 {
		t.Fatalf("calls = %d: client deadlines must not open the breaker", got)
	}
}

func TestGuardedPublisherRecoversAfterHealthCheck(t *testing.T) {
	t.Parallel()

	next := &countingPublisher{err: errors.New("broker down")}
	g := newGuarded(t, next)
	for range 2 {
		_ = g.Publish(context.Background(), domain.Hit{})
	}
	next.err = nil

	// Пока проверка живости не прошла, реальные удары очередь не трогают.
	if err := g.Publish(context.Background(), domain.Hit{}); !errors.Is(err, domain.ErrQueueUnavailable) {
		t.Fatalf("err = %v: traffic must stay blocked until the health check passes", err)
	}

	g.breaker.HalfOpen() // то, что делает Run после успешного Ping
	for range 2 {
		if err := g.Publish(context.Background(), domain.Hit{}); err != nil {
			t.Fatalf("probe err = %v", err)
		}
	}
	if g.breaker.State() != breaker.Closed {
		t.Fatalf("state = %v, want closed", g.breaker.State())
	}
}

func TestPublishErrorKeepsBreakerRejection(t *testing.T) {
	t.Parallel()

	if err := publishError(context.Background(), domain.ErrQueueUnavailable); !errors.Is(err, domain.ErrQueueUnavailable) {
		t.Fatalf("err = %v, want ErrQueueUnavailable", err)
	}
}

// steppingClock сдвигается на секунду при каждом обращении: время в half-open проходит само, тест не спит.
func steppingClock() func() time.Time {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	return func() time.Time {
		now = now.Add(time.Second)
		return now
	}
}
