package watch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type hubFixture struct {
	hub    *Hub
	feeder *fakeFeeder
}

func newFixture(t *testing.T, cfg Config) hubFixture {
	t.Helper()
	feeder := newFakeFeeder()
	hub := New(feeder, cfg)
	t.Cleanup(hub.Close)
	return hubFixture{hub: hub, feeder: feeder}
}

func (fx hubFixture) subscribe(t *testing.T, raidID uuid.UUID) *Subscription {
	t.Helper()
	sub, err := fx.hub.Subscribe(t.Context(), raidID)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(sub.Close)
	return sub
}

func TestHub_DeliversToAllSubscribersOfRaidOnly(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: -1})
	raidA, raidB := uuid.New(), uuid.New()
	a1, a2, b := fx.subscribe(t, raidA), fx.subscribe(t, raidA), fx.subscribe(t, raidB)

	fx.feeder.emit(t, raidA, hpUpdate(42))

	if got := nextHP(t, a1); got != 42 {
		t.Fatalf("a1 got %d, want 42", got)
	}
	if got := nextHP(t, a2); got != 42 {
		t.Fatalf("a2 got %d, want 42", got)
	}
	expectSilence(t, b)
}

func TestHub_OneSourceSubscriptionPerRaid(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: -1})
	raidID := uuid.New()
	first, second := fx.subscribe(t, raidID), fx.subscribe(t, raidID)
	if starts, _ := fx.feeder.counts(raidID); starts != 1 {
		t.Fatalf("starts = %d, want 1 for two subscribers", starts)
	}

	first.Close()
	if _, stops := fx.feeder.counts(raidID); stops != 0 {
		t.Fatalf("stops = %d, want 0 while a subscriber remains", stops)
	}

	second.Close()
	if _, stops := fx.feeder.counts(raidID); stops != 1 {
		t.Fatalf("stops = %d, want 1 after the last subscriber left", stops)
	}

	fx.subscribe(t, raidID)
	if starts, _ := fx.feeder.counts(raidID); starts != 2 {
		t.Fatalf("starts = %d, want 2 after resubscribe", starts)
	}
}

func TestHub_CloseSubscription(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: -1})
	raidID := uuid.New()
	sub := fx.subscribe(t, raidID)
	sub.Close()

	if _, err := sub.Next(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next err = %v, want ErrClosed", err)
	}
	if _, stops := fx.feeder.counts(raidID); stops != 1 {
		t.Fatalf("stops = %d, want 1", stops)
	}
}

func TestHub_CloseHubStopsSubscribersAndSources(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: -1})
	raidID := uuid.New()
	sub := fx.subscribe(t, raidID)

	fx.hub.Close()
	fx.hub.Close() // идемпотентен

	if _, err := sub.Next(t.Context()); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("Next err = %v, want ErrHubClosed", err)
	}
	if _, err := fx.hub.Subscribe(t.Context(), raidID); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("Subscribe after Close err = %v, want ErrHubClosed", err)
	}
	if _, stops := fx.feeder.counts(raidID); stops != 1 {
		t.Fatalf("stops = %d, want 1", stops)
	}
}

func TestHub_SubscribeWithCanceledContextLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: -1})
	raidID := uuid.New()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := fx.hub.Subscribe(ctx, raidID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe err = %v, want context.Canceled", err)
	}
	if starts, _ := fx.feeder.counts(raidID); starts != 0 {
		t.Fatalf("starts = %d, want 0: source must not start for a canceled subscribe", starts)
	}
}

func TestHub_ThrottleCollapsesBurstToLastValue(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: 40 * time.Millisecond})
	raidID := uuid.New()
	sub := fx.subscribe(t, raidID)

	const burst = 50
	for i := int64(1); i <= burst; i++ {
		fx.feeder.emit(t, raidID, hpUpdate(i))
	}

	received := 0
	for {
		received++
		if nextHP(t, sub) == burst {
			break
		}
	}
	if received >= burst/2 {
		t.Fatalf("received %d updates for a burst of %d, want them collapsed", received, burst)
	}
}

func TestHub_FinishedIsImmediateAndDropsThrottledUpdates(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{Interval: time.Hour})
	raidID := uuid.New()
	sub := fx.subscribe(t, raidID)

	fx.feeder.emit(t, raidID, hpUpdate(10)) // сразу
	fx.feeder.emit(t, raidID, hpUpdate(0))  // ждёт часового интервала
	fx.feeder.emit(t, raidID, finishedUpdate())

	if got := nextHP(t, sub); got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
	if u := nextUpdate(t, sub); u.GetFinished() == nil {
		t.Fatalf("got %v, want finished", u)
	}
	expectSilence(t, sub)
}

func TestHub_BufferedUpdateSurvivesFirstOverflow(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{BufferSize: 2, Interval: -1})
	raidID := uuid.New()
	sub := fx.subscribe(t, raidID)

	for i := int64(1); i <= 3; i++ {
		fx.feeder.emit(t, raidID, hpUpdate(i))
	}

	for want := int64(1); want <= 3; want++ {
		if got := nextHP(t, sub); got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	}
}

func TestHub_SlowSubscriberIsDroppedWithoutDelayingOthers(t *testing.T) {
	t.Parallel()
	fx := newFixture(t, Config{BufferSize: 2, Interval: -1})
	raidID := uuid.New()
	slow, fast := fx.subscribe(t, raidID), fx.subscribe(t, raidID)

	// Быстрый читает каждое обновление сразу, медленный не читает вовсе.
	const updates = 8
	for i := int64(1); i <= updates; i++ {
		fx.feeder.emit(t, raidID, hpUpdate(i))
		if got := nextHP(t, fast); got != i {
			t.Fatalf("fast got %d, want %d", got, i)
		}
	}

	// Буфер 2 + pending 1: четвёртое обновление переполняет повторно.
	if _, err := slow.Next(t.Context()); !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("slow Next err = %v, want ErrSlowConsumer", err)
	}
	fx.feeder.emit(t, raidID, hpUpdate(updates+1))
	if got := nextHP(t, fast); got != updates+1 {
		t.Fatalf("fast got %d, want %d after the slow one was dropped", got, updates+1)
	}
}
