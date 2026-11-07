package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/health"
)

const testDLQTopic = "raid.hits.dlq"

func startConsumerCluster(t *testing.T) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(testPartitions, testTopic),
		kfake.SeedTopics(1, testDLQTopic),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

// fakeProcessor запоминает удары; fail задаёт, сколько первых вызовов вернут ошибку.
type fakeProcessor struct {
	mu       sync.Mutex
	hits     []domain.Hit
	calls    int
	fail     int
	rejectID uuid.UUID
}

func (f *fakeProcessor) ProcessBatch(_ context.Context, hits []domain.Hit) (domain.BatchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.fail {
		return domain.BatchResult{}, errors.New("db down")
	}
	var res domain.BatchResult
	for _, h := range hits {
		if h.ID == f.rejectID {
			res.Rejected = append(res.Rejected, domain.RejectedHits{
				RaidID: h.RaidID, Hits: []domain.Hit{h}, Reason: "player not found",
			})
			continue
		}
		f.hits = append(f.hits, h)
	}
	return res, nil
}

func (f *fakeProcessor) seen() []domain.Hit {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.Hit(nil), f.hits...)
}

func newConsumer(t *testing.T, brokers []string, proc HitProcessor) *Consumer {
	t.Helper()

	c, err := NewConsumer(ConsumerConfig{
		Brokers: brokers, Topic: testTopic, DLQTopic: testDLQTopic, Group: "test-group",
		BatchSize: 500, BatchTimeout: 20 * time.Millisecond,
	}, proc)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	return c
}

// startConsumer запускает Run и возвращает stop, который останавливает консьюмер и ждёт выхода.
func startConsumer(t *testing.T, c *Consumer) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := c.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
			c.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		<-time.After(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func readDLQ(t *testing.T, brokers []string, want int) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(testDLQTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("dlq client: %v", err)
	}
	defer client.Close()

	var got []*kgo.Record
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < want && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		client.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got = append(got, r) })
		cancel()
	}
	return got
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func produceRaw(t *testing.T, brokers []string, key string, value []byte) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(testTopic))
	if err != nil {
		t.Fatalf("raw producer: %v", err)
	}
	defer client.Close()
	if err := client.ProduceSync(context.Background(), &kgo.Record{Key: []byte(key), Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce raw: %v", err)
	}
}

func publishHits(t *testing.T, brokers []string, hits ...domain.Hit) {
	t.Helper()

	p := newProducer(t, brokers)
	for _, h := range hits {
		if err := p.Publish(context.Background(), h); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
}

func TestConsumer_ProcessesHitsInOrderAndCommits(t *testing.T) {
	t.Parallel()

	brokers := startConsumerCluster(t)
	raidID := uuid.New()
	sent := make([]domain.Hit, 0, 25)
	for range 25 {
		sent = append(sent, newHit(raidID))
	}
	publishHits(t, brokers, sent...)

	first := &fakeProcessor{}
	stop := startConsumer(t, newConsumer(t, brokers, first))
	eventually(t, "all hits processed", func() bool { return len(first.seen()) == len(sent) })

	for i, h := range first.seen() {
		if h.ID != sent[i].ID {
			t.Fatalf("hit #%d out of order", i)
		}
	}
	stop()

	// Offset закоммичен: новый консьюмер той же группы не получает уже обработанное.
	second := &fakeProcessor{}
	startConsumer(t, newConsumer(t, brokers, second))
	extra := newHit(raidID)
	publishHits(t, brokers, extra)
	eventually(t, "new hit processed", func() bool { return len(second.seen()) >= 1 })
	if got := second.seen(); len(got) != 1 || got[0].ID != extra.ID {
		t.Fatalf("second consumer saw %d hits, want only the new one", len(got))
	}
}

func TestConsumer_BrokenMessageGoesToDLQ(t *testing.T) {
	t.Parallel()

	brokers := startConsumerCluster(t)
	raidID := uuid.New()
	good := newHit(raidID)
	garbage := []byte("not a protobuf \xff\xff")
	produceRaw(t, brokers, raidID.String(), garbage)
	publishHits(t, brokers, good)

	proc := &fakeProcessor{}
	startConsumer(t, newConsumer(t, brokers, proc))

	eventually(t, "good hit processed after garbage", func() bool { return len(proc.seen()) == 1 })
	dlq := readDLQ(t, brokers, 1)
	if len(dlq) != 1 || string(dlq[0].Value) != string(garbage) {
		t.Fatalf("dlq records = %d, want the original bytes", len(dlq))
	}
	if reason := header(dlq[0], headerDLQReason); reason == "" || header(dlq[0], headerSourceTopic) != testTopic {
		t.Fatalf("dlq headers: reason=%q source=%q", reason, header(dlq[0], headerSourceTopic))
	}
}

func TestConsumer_RejectedHitGoesToDLQ(t *testing.T) {
	t.Parallel()

	brokers := startConsumerCluster(t)
	raidID := uuid.New()
	bad, good := newHit(raidID), newHit(raidID)
	publishHits(t, brokers, bad, good)

	proc := &fakeProcessor{rejectID: bad.ID}
	startConsumer(t, newConsumer(t, brokers, proc))

	eventually(t, "good hit processed", func() bool { return len(proc.seen()) == 1 })
	dlq := readDLQ(t, brokers, 1)
	if len(dlq) != 1 || header(dlq[0], headerDLQReason) != "rejected: player not found" {
		t.Fatalf("dlq = %d records, reason %q", len(dlq), header(dlq[0], headerDLQReason))
	}
}

func TestConsumer_RetriesBatchUntilProcessorRecovers(t *testing.T) {
	t.Parallel()

	brokers := startConsumerCluster(t)
	publishHits(t, brokers, newHit(uuid.New()))

	proc := &fakeProcessor{fail: 2}
	startConsumer(t, newConsumer(t, brokers, proc))

	eventually(t, "hit processed after retries", func() bool { return len(proc.seen()) == 1 })
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if proc.calls != 3 {
		t.Fatalf("ProcessBatch calls = %d, want 3 (2 failures + success)", proc.calls)
	}
}

func TestNewConsumer_Validation(t *testing.T) {
	t.Parallel()

	ok := ConsumerConfig{
		Brokers: []string{"localhost:1"}, Topic: "t", DLQTopic: "d", Group: "g",
		BatchSize: 1, BatchTimeout: time.Millisecond,
	}
	for name, mutate := range map[string]func(*ConsumerConfig){
		"no brokers":    func(c *ConsumerConfig) { c.Brokers = nil },
		"no group":      func(c *ConsumerConfig) { c.Group = "" },
		"no dlq":        func(c *ConsumerConfig) { c.DLQTopic = "" },
		"zero batch":    func(c *ConsumerConfig) { c.BatchSize = 0 },
		"zero deadline": func(c *ConsumerConfig) { c.BatchTimeout = 0 },
	} {
		cfg := ok
		mutate(&cfg)
		if _, err := NewConsumer(cfg, &fakeProcessor{}); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestConsumer_HeartbeatWhileIdleAndRetrying(t *testing.T) {
	t.Parallel()

	brokers := startConsumerCluster(t)
	hb := &health.Heartbeat{}
	proc := &fakeProcessor{fail: 3}
	c, err := NewConsumer(ConsumerConfig{
		Brokers: brokers, Topic: testTopic, DLQTopic: testDLQTopic, Group: "test-group",
		BatchSize: 500, BatchTimeout: 20 * time.Millisecond, Heartbeat: hb,
	}, proc)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.pollTimeout = 50 * time.Millisecond
	startConsumer(t, c)

	// Топик пуст: PollFetches не возвращает записей, но цикл опроса всё равно отмечается.
	first := time.Now()
	eventually(t, "heartbeat while idle", func() bool { return hb.Last().After(first) })

	publishHits(t, brokers, newHit(uuid.New()))
	eventually(t, "hit processed after retries", func() bool { return len(proc.seen()) == 1 })
	if since := time.Since(hb.Last()); since > time.Second {
		t.Fatalf("last heartbeat %v ago, want recent", since)
	}
}

// hangingOnceProcessor в первом вызове не отвечает, пока не истечёт контекст (Postgres на паузе).
type hangingOnceProcessor struct {
	fakeProcessor
	hung atomic.Bool
}

func (p *hangingOnceProcessor) ProcessBatch(ctx context.Context, hits []domain.Hit) (domain.BatchResult, error) {
	if p.hung.CompareAndSwap(false, true) {
		<-ctx.Done()
		return domain.BatchResult{}, ctx.Err()
	}
	return p.fakeProcessor.ProcessBatch(ctx, hits)
}

func TestConsumer_AttemptTimeoutRetriesHangingBatch(t *testing.T) {
	t.Parallel()

	brokers := startConsumerCluster(t)
	publishHits(t, brokers, newHit(uuid.New()))

	proc := &hangingOnceProcessor{}
	c := newConsumer(t, brokers, proc)
	c.attemptTimeout = 50 * time.Millisecond
	startConsumer(t, c)

	eventually(t, "hit processed after the hanging attempt timed out", func() bool { return len(proc.seen()) == 1 })
}
