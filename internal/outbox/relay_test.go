package outbox

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/health"
)

// fakeStore имитирует таблицу outbox. Откат транзакции моделируется снимком sent на входе.
type fakeStore struct {
	mu      sync.Mutex
	entries []domain.OutboxEntry
	sent    map[int64]bool
}

func newFakeStore(n int) *fakeStore {
	s := &fakeStore{sent: map[int64]bool{}}
	for i := range n {
		s.entries = append(s.entries, domain.OutboxEntry{
			ID: int64(i + 1), Topic: "raid.events", Key: "raid", Payload: []byte{byte(i)},
		})
	}
	return s
}

func (s *fakeStore) LockUnsent(_ context.Context, limit int) ([]domain.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.OutboxEntry
	for _, e := range s.entries {
		if !s.sent[e.ID] && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) MarkSent(_ context.Context, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.sent[id] = true
	}
	return nil
}

func (s *fakeStore) CountUnsent(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.entries) - len(s.sent)), nil
}

func (s *fakeStore) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

// fakeTx выполняет fn; при ошибке откатывает пометки sent, сделанные внутри.
type fakeTx struct{ store *fakeStore }

func (t fakeTx) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	t.store.mu.Lock()
	snapshot := maps.Clone(t.store.sent)
	t.store.mu.Unlock()

	if err := fn(ctx); err != nil {
		t.store.mu.Lock()
		t.store.sent = snapshot
		t.store.mu.Unlock()
		return err
	}
	return nil
}

type fakePublisher struct {
	mu      sync.Mutex
	batches [][]domain.OutboxMessage
	failFor int // сколько первых вызовов завершить ошибкой
	calls   int
}

func (p *fakePublisher) PublishBatch(_ context.Context, msgs []domain.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failFor {
		return errors.New("broker unavailable")
	}
	p.batches = append(p.batches, slices.Clone(msgs))
	return nil
}

func (p *fakePublisher) published() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, b := range p.batches {
		n += len(b)
	}
	return n
}

func newTestRelay(t *testing.T, store *fakeStore, pub *fakePublisher, batch int) *Relay {
	t.Helper()
	r, err := NewRelay(fakeTx{store}, store, pub, Config{BatchSize: batch, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	return r
}

func TestNewRelayValidatesConfig(t *testing.T) {
	t.Parallel()

	store := newFakeStore(0)
	for _, cfg := range []Config{
		{BatchSize: 0, PollInterval: time.Second},
		{BatchSize: 10, PollInterval: 0},
	} {
		if _, err := NewRelay(fakeTx{store}, store, &fakePublisher{}, cfg); err == nil {
			t.Fatalf("NewRelay(%+v) succeeded, want error", cfg)
		}
	}
}

func TestRelayBatch_PublishesAndMarksInOrder(t *testing.T) {
	t.Parallel()

	store, pub := newFakeStore(5), &fakePublisher{}
	relay := newTestRelay(t, store, pub, 3)

	n, err := relay.relayBatch(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("relayBatch = %d, %v; want 3, nil", n, err)
	}
	if store.sentCount() != 3 || pub.published() != 3 {
		t.Fatalf("sent=%d published=%d, want 3 and 3", store.sentCount(), pub.published())
	}
	if got := pub.batches[0][0].Payload[0]; got != 0 {
		t.Fatalf("first record payload = %d, records must go in id order", got)
	}

	n, err = relay.relayBatch(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("second relayBatch = %d, %v; want 2, nil", n, err)
	}
	n, err = relay.relayBatch(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("empty relayBatch = %d, %v; want 0, nil", n, err)
	}
}

// Ошибка брокера откатывает транзакцию: записи остаются неотправленными и уйдут при следующей попытке.
func TestRelayBatch_PublishErrorLeavesRecordsUnsent(t *testing.T) {
	t.Parallel()

	store, pub := newFakeStore(2), &fakePublisher{failFor: 1}
	relay := newTestRelay(t, store, pub, 10)

	if _, err := relay.relayBatch(context.Background()); err == nil {
		t.Fatal("relayBatch succeeded, want publish error")
	}
	if store.sentCount() != 0 {
		t.Fatalf("sent = %d after failed publish, want 0", store.sentCount())
	}

	if n, err := relay.relayBatch(context.Background()); err != nil || n != 2 {
		t.Fatalf("retry = %d, %v; want 2, nil", n, err)
	}
}

func TestRun_DrainsQueueAndRetriesAfterErrors(t *testing.T) {
	t.Parallel()

	store, pub := newFakeStore(25), &fakePublisher{failFor: 2}
	relay := newTestRelay(t, store, pub, 10)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for store.sentCount() < 25 {
		select {
		case <-deadline:
			t.Fatalf("sent %d of 25 records", store.sentCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if pub.published() != 25 {
		t.Fatalf("published = %d, want 25 (no duplicates without crashes)", pub.published())
	}
}

func TestRun_StopsOnCancelWhenIdle(t *testing.T) {
	t.Parallel()

	store := newFakeStore(0)
	relay := newTestRelay(t, store, &fakePublisher{}, 10)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	<-time.After(20 * time.Millisecond)
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

// Отмена контекста посреди публикации не должна обрывать транзакцию: пачка уже ушла в Kafka,
// и без пометки отправленной она ушла бы повторно.
func TestRelayBatch_CancelDoesNotAbortInFlightBatch(t *testing.T) {
	t.Parallel()

	store := newFakeStore(3)
	ctx, cancel := context.WithCancel(context.Background())
	pub := &cancelingPublisher{cancel: cancel}
	relay, err := NewRelay(fakeTx{store}, store, pub, Config{BatchSize: 10, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	if err := relay.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if store.sentCount() != 3 {
		t.Fatalf("sent = %d, want 3: in-flight batch must be committed", store.sentCount())
	}
}

type cancelingPublisher struct{ cancel context.CancelFunc }

func (p *cancelingPublisher) PublishBatch(ctx context.Context, _ []domain.OutboxMessage) error {
	p.cancel() // сигнал остановки приходит во время публикации
	return ctx.Err()
}

func TestRun_BeatsHeartbeatWhileIdle(t *testing.T) {
	t.Parallel()

	store := newFakeStore(0)
	hb := &health.Heartbeat{}
	r, err := NewRelay(fakeTx{store}, store, &fakePublisher{}, Config{
		BatchSize: 10, PollInterval: 5 * time.Millisecond, Heartbeat: hb,
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	first := time.Now()
	deadline := time.After(2 * time.Second)
	for !hb.Last().After(first.Add(20 * time.Millisecond)) {
		select {
		case <-deadline:
			t.Fatal("relay did not keep beating while the queue is empty")
		case <-time.After(time.Millisecond):
		}
	}
}

// hangingPublisher не отвечает, пока не истечёт контекст: так ведут себя Kafka или Postgres,
// которые молчат, а не рвут соединение.
type hangingPublisher struct {
	mu    sync.Mutex
	calls int
}

func (p *hangingPublisher) PublishBatch(ctx context.Context, _ []domain.OutboxMessage) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (p *hangingPublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func TestRun_BatchTimeoutKeepsLoopAlive(t *testing.T) {
	t.Parallel()

	store := newFakeStore(3)
	pub := &hangingPublisher{}
	r, err := NewRelay(fakeTx{store}, store, pub, Config{
		BatchSize: 10, PollInterval: 5 * time.Millisecond, BatchTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for pub.callCount() < 2 {
		select {
		case <-deadline:
			t.Fatalf("hanging batch was not cut by BatchTimeout: %d calls", pub.callCount())
		case <-time.After(time.Millisecond):
		}
	}
	if store.sentCount() != 0 {
		t.Fatalf("timed out batch must roll back, sent = %d", store.sentCount())
	}
}
