package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/health"
)

type fakeTx struct{}

func (fakeTx) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error { return fn(ctx) }

type fakeExpiry struct {
	mu      sync.Mutex
	calls   int
	failFor int // сколько первых вызовов завершить ошибкой
	due     []domain.ExpiredRaid
}

func (f *fakeExpiry) ExpireDue(context.Context) ([]domain.ExpiredRaid, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failFor {
		return nil, errors.New("db down")
	}
	out := f.due
	f.due = nil // рейд отдаётся один раз, как при UPDATE ... WHERE status = 'active'
	return out, nil
}

func (f *fakeExpiry) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeOutbox struct {
	mu   sync.Mutex
	msgs []domain.OutboxMessage
}

func (o *fakeOutbox) Add(_ context.Context, msg domain.OutboxMessage) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.msgs = append(o.msgs, msg)
	return nil
}

func (o *fakeOutbox) messages() []domain.OutboxMessage {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]domain.OutboxMessage(nil), o.msgs...)
}

func newTestScheduler(t *testing.T, expiry *fakeExpiry, outbox *fakeOutbox) *Scheduler {
	t.Helper()

	s, err := New(fakeTx{}, expiry, outbox, Config{Interval: 5 * time.Millisecond, EventsTopic: "raid.events"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	for _, cfg := range []Config{
		{Interval: 0, EventsTopic: "raid.events"},
		{Interval: time.Second, EventsTopic: ""},
	} {
		if _, err := New(fakeTx{}, &fakeExpiry{}, &fakeOutbox{}, cfg); err == nil {
			t.Fatalf("New(%+v) succeeded, want error", cfg)
		}
	}
}

func TestExpireOnce_WritesExpiredEventToOutbox(t *testing.T) {
	t.Parallel()

	raidID := uuid.New()
	finished := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	expiry := &fakeExpiry{due: []domain.ExpiredRaid{{ID: raidID, HPLeft: 42, FinishedAt: finished}}}
	outbox := &fakeOutbox{}
	s := newTestScheduler(t, expiry, outbox)

	n, err := s.ExpireOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("ExpireOnce = %d, %v; want 1, nil", n, err)
	}

	msgs := outbox.messages()
	if len(msgs) != 1 || msgs[0].Topic != "raid.events" || msgs[0].Key != raidID.String() ||
		msgs[0].Headers["type"] != "RaidExpired" {
		t.Fatalf("outbox = %+v", msgs)
	}
	var ev eventsV1.RaidEvent
	if err := proto.Unmarshal(msgs[0].Payload, &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.GetRaidId() != raidID.String() || ev.GetExpired().GetHpLeft() != 42 ||
		!ev.GetOccurredAt().AsTime().Equal(finished) {
		t.Fatalf("event = %+v", &ev)
	}

	if n, err := s.ExpireOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("second ExpireOnce = %d, %v; want 0, nil", n, err)
	}
	if len(outbox.messages()) != 1 {
		t.Fatal("second pass must not add events")
	}
}

// Ошибка прохода не останавливает цикл: следующий тик повторяет работу.
func TestRun_ContinuesAfterErrors(t *testing.T) {
	t.Parallel()

	expiry := &fakeExpiry{failFor: 2, due: []domain.ExpiredRaid{{ID: uuid.New()}}}
	outbox := &fakeOutbox{}
	s := newTestScheduler(t, expiry, outbox)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for len(outbox.messages()) == 0 {
		select {
		case <-deadline:
			t.Fatalf("no event after %d passes", expiry.callCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if expiry.callCount() < 3 {
		t.Fatalf("passes = %d, want at least 3 (2 failures + success)", expiry.callCount())
	}
}

func TestRun_FirstPassIsImmediateAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	expiry := &fakeExpiry{}
	s, err := New(fakeTx{}, expiry, &fakeOutbox{}, Config{Interval: time.Hour, EventsTopic: "raid.events"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for expiry.callCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("first pass did not run without waiting for the interval")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

type fakePublisher struct {
	mu  sync.Mutex
	ids []uuid.UUID
	err error
}

func (p *fakePublisher) PublishExpired(_ context.Context, raidID uuid.UUID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = append(p.ids, raidID)
	return p.err
}

// Подписчиков WatchRaid уведомляют о каждом истёкшем рейде, а ошибка уведомления проход не ломает.
func TestExpireOnce_NotifiesWatchers(t *testing.T) {
	t.Parallel()

	first, second := uuid.New(), uuid.New()
	expiry := &fakeExpiry{due: []domain.ExpiredRaid{{ID: first}, {ID: second}}}
	pub := &fakePublisher{err: errors.New("redis down")}
	s, err := New(fakeTx{}, expiry, &fakeOutbox{},
		Config{Interval: time.Second, EventsTopic: "raid.events", Publisher: pub})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n, err := s.ExpireOnce(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("ExpireOnce = %d, %v; want 2, nil despite the publisher failing", n, err)
	}
	if len(pub.ids) != 2 || pub.ids[0] != first || pub.ids[1] != second {
		t.Fatalf("notified = %v, want both raids in order", pub.ids)
	}
}

func TestRun_BeatsHeartbeatOnEveryPass(t *testing.T) {
	t.Parallel()

	expiry := &fakeExpiry{}
	hb := &health.Heartbeat{}
	s, err := New(fakeTx{}, expiry, &fakeOutbox{}, Config{
		Interval: 5 * time.Millisecond, EventsTopic: "raid.events", Heartbeat: hb,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for expiry.callCount() < 3 {
		select {
		case <-deadline:
			t.Fatal("scheduler did not make 3 passes")
		case <-time.After(time.Millisecond):
		}
	}
	if since := time.Since(hb.Last()); since > time.Second {
		t.Fatalf("last heartbeat %v ago, want a beat on every pass", since)
	}
}

// hangingExpiry не отвечает, пока не истечёт контекст: так ведёт себя Postgres на паузе.
type hangingExpiry struct {
	mu    sync.Mutex
	calls int
}

func (f *hangingExpiry) ExpireDue(ctx context.Context) ([]domain.ExpiredRaid, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *hangingExpiry) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestRun_PassTimeoutKeepsLoopAlive(t *testing.T) {
	t.Parallel()

	expiry := &hangingExpiry{}
	hb := &health.Heartbeat{}
	s, err := New(fakeTx{}, expiry, &fakeOutbox{}, Config{
		Interval: 5 * time.Millisecond, EventsTopic: "raid.events", Heartbeat: hb, PassTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for expiry.callCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("hanging pass was not cut by PassTimeout: %d calls", expiry.callCount())
		case <-time.After(time.Millisecond):
		}
	}
	if since := time.Since(hb.Last()); since > time.Second {
		t.Fatalf("last heartbeat %v ago, want the loop to keep beating", since)
	}
}
