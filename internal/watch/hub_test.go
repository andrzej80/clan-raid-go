package watch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Одни и те же тесты гоняются на обеих реализациях Hub: поведение должно совпадать.

type hubFactory func(Feeder, Config) Hub

var hubImpls = []struct {
	name string
	make hubFactory
}{
	{"actor", func(f Feeder, c Config) Hub { return NewActorHub(f, c) }},
	{"mutex", func(f Feeder, c Config) Hub { return NewMutexHub(f, c) }},
}

type hubFixture struct {
	hub    Hub
	feeder *fakeFeeder
}

// forEachHub запускает тест на каждой реализации с её собственным hub и источником.
func forEachHub(t *testing.T, cfg Config, fn func(t *testing.T, fx hubFixture)) {
	t.Helper()
	for _, impl := range hubImpls {
		t.Run(impl.name, func(t *testing.T) {
			t.Parallel()
			feeder := newFakeFeeder()
			hub := impl.make(feeder, cfg)
			t.Cleanup(hub.Close)
			fn(t, hubFixture{hub: hub, feeder: feeder})
		})
	}
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

// barrier возвращается, когда hub обработал всё, что было отправлено до него. У ActorHub
// сообщения идут через очередь, поэтому для проверки «после всего» нужна точка синхронизации:
// проба по отдельному рейду проходит через ту же очередь.
func (fx hubFixture) barrier(t *testing.T) {
	t.Helper()
	probeID := uuid.New()
	probe, err := fx.hub.Subscribe(t.Context(), probeID)
	if err != nil {
		t.Fatalf("barrier Subscribe: %v", err)
	}
	fx.feeder.emit(t, probeID, hpUpdate(0))
	nextHP(t, probe)
	probe.Close()
}

func TestHub_DeliversToAllSubscribersOfRaidOnly(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: -1}, func(t *testing.T, fx hubFixture) {
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
	})
}

func TestHub_OneSourceSubscriptionPerRaid(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: -1}, func(t *testing.T, fx hubFixture) {
		raidID := uuid.New()
		first, second := fx.subscribe(t, raidID), fx.subscribe(t, raidID)
		if starts, _ := fx.feeder.counts(raidID); starts != 1 {
			t.Fatalf("starts = %d, want 1 for two subscribers", starts)
		}

		first.Close()
		fx.barrier(t)
		if _, stops := fx.feeder.counts(raidID); stops != 0 {
			t.Fatalf("stops = %d, want 0 while a subscriber remains", stops)
		}

		second.Close()
		eventually(t, func() bool { _, stops := fx.feeder.counts(raidID); return stops == 1 })

		fx.subscribe(t, raidID)
		if starts, _ := fx.feeder.counts(raidID); starts != 2 {
			t.Fatalf("starts = %d, want 2 after resubscribe", starts)
		}
	})
}

func TestHub_CloseSubscription(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: -1}, func(t *testing.T, fx hubFixture) {
		raidID := uuid.New()
		sub := fx.subscribe(t, raidID)
		sub.Close()

		if _, err := sub.Next(t.Context()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Next err = %v, want ErrClosed", err)
		}
		eventually(t, func() bool { _, stops := fx.feeder.counts(raidID); return stops == 1 })
	})
}

func TestHub_CloseHubStopsSubscribersAndSources(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: -1}, func(t *testing.T, fx hubFixture) {
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
	})
}

func TestHub_SubscribeWithCanceledContextLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: -1}, func(t *testing.T, fx hubFixture) {
		raidID := uuid.New()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if _, err := fx.hub.Subscribe(ctx, raidID); !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe err = %v, want context.Canceled", err)
		}
		eventually(t, func() bool {
			starts, stops := fx.feeder.counts(raidID)
			return starts == stops
		})
	})
}

func TestHub_ThrottleCollapsesBurstToLastValue(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: 40 * time.Millisecond}, func(t *testing.T, fx hubFixture) {
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
	})
}

func TestHub_FinishedIsImmediateAndDropsThrottledUpdates(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{Interval: time.Hour}, func(t *testing.T, fx hubFixture) {
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
	})
}

func TestHub_BufferedUpdateSurvivesFirstOverflow(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{BufferSize: 2, Interval: -1}, func(t *testing.T, fx hubFixture) {
		raidID := uuid.New()
		sub := fx.subscribe(t, raidID)

		for i := int64(1); i <= 3; i++ {
			fx.feeder.emit(t, raidID, hpUpdate(i))
		}
		fx.barrier(t)

		for want := int64(1); want <= 3; want++ {
			if got := nextHP(t, sub); got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		}
	})
}

func TestHub_SlowSubscriberIsDroppedWithoutDelayingOthers(t *testing.T) {
	t.Parallel()
	forEachHub(t, Config{BufferSize: 2, Interval: -1}, func(t *testing.T, fx hubFixture) {
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
	})
}
