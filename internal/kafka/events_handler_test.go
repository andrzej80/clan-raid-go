package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

const (
	testEventsTopic    = "raid.events"
	testEventsDLQTopic = "raid.events.dlq"
)

func defeatedEvent(raidID uuid.UUID) *eventsV1.RaidEvent {
	return &eventsV1.RaidEvent{
		RaidId: raidID.String(),
		Event: &eventsV1.RaidEvent_Defeated{Defeated: &eventsV1.RaidDefeated{
			KillerPlayerId: 7, KillerHitId: uuid.NewString(),
		}},
	}
}

func expiredEvent(raidID uuid.UUID) *eventsV1.RaidEvent {
	return &eventsV1.RaidEvent{
		RaidId: raidID.String(),
		Event:  &eventsV1.RaidEvent_Expired{Expired: &eventsV1.RaidExpired{HpLeft: 10}},
	}
}

func TestDecodeRaidEvent(t *testing.T) {
	t.Parallel()

	raidID := uuid.New()
	mustMarshal := func(m proto.Message) []byte {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}

	tests := []struct {
		name    string
		value   []byte
		want    domain.RaidEvent
		wantErr bool
	}{
		{"defeated", mustMarshal(defeatedEvent(raidID)), domain.RaidEvent{RaidID: raidID, Kind: domain.RaidEventDefeated}, false},
		{"expired", mustMarshal(expiredEvent(raidID)), domain.RaidEvent{RaidID: raidID, Kind: domain.RaidEventExpired}, false},
		{"garbage", []byte("not a protobuf \xff\xff"), domain.RaidEvent{}, true},
		{"bad raid id", mustMarshal(&eventsV1.RaidEvent{RaidId: "nope", Event: expiredEvent(raidID).Event}), domain.RaidEvent{}, true},
		{"event not set", mustMarshal(&eventsV1.RaidEvent{RaidId: raidID.String()}), domain.RaidEvent{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeRaidEvent(tc.value)
			if (err != nil) != tc.wantErr {
				t.Fatalf("decodeRaidEvent error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("decodeRaidEvent = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// fakeEventProcessor запоминает события; fail задаёт, сколько первых вызовов вернут ошибку,
// rejectID — рейд, события которого отклоняются.
type fakeEventProcessor struct {
	mu       sync.Mutex
	events   []domain.RaidEvent
	calls    int
	fail     int
	rejectID uuid.UUID
}

func (f *fakeEventProcessor) ProcessEvents(_ context.Context, events []domain.RaidEvent) ([]domain.RejectedEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.fail {
		return nil, errors.New("db down")
	}
	var rejected []domain.RejectedEvent
	for i, ev := range events {
		if ev.RaidID == f.rejectID {
			rejected = append(rejected, domain.RejectedEvent{Index: i, Reason: "raid is not defeated"})
			continue
		}
		f.events = append(f.events, ev)
	}
	return rejected, nil
}

func (f *fakeEventProcessor) seen() []domain.RaidEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.RaidEvent(nil), f.events...)
}

func startEventsCluster(t *testing.T) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(6, testEventsTopic),
		kfake.SeedTopics(1, testEventsDLQTopic),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func produceEvents(t *testing.T, brokers []string, values ...[]byte) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(testEventsTopic))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer client.Close()
	for i, v := range values {
		// Один ключ: все записи попадают в одну партицию и обрабатываются по порядку.
		rec := &kgo.Record{Key: []byte("raid"), Value: v}
		if err := client.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
			t.Fatalf("produce #%d: %v", i, err)
		}
	}
}

func marshalEvent(t *testing.T, ev *eventsV1.RaidEvent) []byte {
	t.Helper()

	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func newEventsConsumer(t *testing.T, brokers []string, proc RaidEventProcessor) *Consumer {
	t.Helper()

	c, err := NewRaidEventsConsumer(ConsumerConfig{
		Brokers: brokers, Topic: testEventsTopic, DLQTopic: testEventsDLQTopic, Group: "rewards-test",
		BatchSize: 500, BatchTimeout: 20 * time.Millisecond,
	}, proc)
	if err != nil {
		t.Fatalf("NewRaidEventsConsumer: %v", err)
	}
	return c
}

func readEventsDLQ(t *testing.T, brokers []string, want int) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(testEventsDLQTopic),
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

func TestRaidEventsConsumer_ProcessesEventsInOrder(t *testing.T) {
	t.Parallel()

	brokers := startEventsCluster(t)
	a, b := uuid.New(), uuid.New()
	produceEvents(t, brokers, marshalEvent(t, defeatedEvent(a)), marshalEvent(t, expiredEvent(b)))

	proc := &fakeEventProcessor{}
	startConsumer(t, newEventsConsumer(t, brokers, proc))

	eventually(t, "both events processed", func() bool { return len(proc.seen()) == 2 })
	got := proc.seen()
	if got[0] != (domain.RaidEvent{RaidID: a, Kind: domain.RaidEventDefeated}) ||
		got[1] != (domain.RaidEvent{RaidID: b, Kind: domain.RaidEventExpired}) {
		t.Fatalf("events = %+v, want defeated(a), expired(b) in order", got)
	}
}

func TestRaidEventsConsumer_BrokenEventGoesToDLQ(t *testing.T) {
	t.Parallel()

	brokers := startEventsCluster(t)
	garbage := []byte("not a protobuf \xff\xff")
	good := uuid.New()
	produceEvents(t, brokers, garbage, marshalEvent(t, defeatedEvent(good)))

	proc := &fakeEventProcessor{}
	startConsumer(t, newEventsConsumer(t, brokers, proc))

	eventually(t, "good event processed after garbage", func() bool { return len(proc.seen()) == 1 })
	dlq := readEventsDLQ(t, brokers, 1)
	if len(dlq) != 1 || string(dlq[0].Value) != string(garbage) {
		t.Fatalf("dlq records = %d, want the original bytes", len(dlq))
	}
	if header(dlq[0], headerSourceTopic) != testEventsTopic || header(dlq[0], headerDLQReason) == "" {
		t.Fatalf("dlq headers: reason=%q source=%q", header(dlq[0], headerDLQReason), header(dlq[0], headerSourceTopic))
	}
}

// Отклонённое процессором событие уходит в DLQ именно своей записью, а не соседней.
func TestRaidEventsConsumer_RejectedEventGoesToDLQ(t *testing.T) {
	t.Parallel()

	brokers := startEventsCluster(t)
	bad, good := uuid.New(), uuid.New()
	badValue := marshalEvent(t, defeatedEvent(bad))
	produceEvents(t, brokers, marshalEvent(t, defeatedEvent(good)), badValue)

	proc := &fakeEventProcessor{rejectID: bad}
	startConsumer(t, newEventsConsumer(t, brokers, proc))

	eventually(t, "good event processed", func() bool { return len(proc.seen()) == 1 })
	dlq := readEventsDLQ(t, brokers, 1)
	if len(dlq) != 1 || string(dlq[0].Value) != string(badValue) ||
		header(dlq[0], headerDLQReason) != "rejected: raid is not defeated" {
		t.Fatalf("dlq = %d records, reason %q", len(dlq), header(dlq[0], headerDLQReason))
	}
}

func TestRaidEventsConsumer_RetriesBatchUntilProcessorRecovers(t *testing.T) {
	t.Parallel()

	brokers := startEventsCluster(t)
	produceEvents(t, brokers, marshalEvent(t, defeatedEvent(uuid.New())))

	proc := &fakeEventProcessor{fail: 2}
	startConsumer(t, newEventsConsumer(t, brokers, proc))

	eventually(t, "event processed after retries", func() bool { return len(proc.seen()) == 1 })
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if proc.calls != 3 {
		t.Fatalf("ProcessEvents calls = %d, want 3 (2 failures + success)", proc.calls)
	}
}
