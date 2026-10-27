package kafka

import (
	"context"
	"errors"
	"net"
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
	testTopic      = "raid.hits"
	testPartitions = 12
)

// startCluster поднимает in-process Kafka (kfake): Docker не нужен.
func startCluster(t *testing.T) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(testPartitions, testTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func newProducer(t *testing.T, brokers []string, extra ...kgo.Opt) *Producer {
	t.Helper()

	p, err := NewProducer(Config{Brokers: brokers, Topic: testTopic}, extra...)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})
	return p
}

func newHit(raidID uuid.UUID) domain.Hit {
	return domain.Hit{
		ID: uuid.New(), RaidID: raidID, PlayerID: 10, ClanID: 1, Damage: 555, IsCrit: true,
		CreatedAt: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC),
	}
}

// consume читает из топика ровно want записей.
func consume(t *testing.T, brokers []string, want int) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(testTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) < want {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("got %d of %d records: %v", len(records), want, err)
		}
		fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
	}
	return records
}

func TestProducer_PublishWritesHitEvent(t *testing.T) {
	t.Parallel()

	brokers := startCluster(t)
	producer := newProducer(t, brokers)
	hit := newHit(uuid.New())

	if err := producer.Publish(context.Background(), hit); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rec := consume(t, brokers, 1)[0]
	if string(rec.Key) != hit.RaidID.String() {
		t.Fatalf("key = %q, want raid_id %s", rec.Key, hit.RaidID)
	}
	var event eventsV1.HitEvent
	if err := proto.Unmarshal(rec.Value, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if event.GetHitId() != hit.ID.String() || event.GetRaidId() != hit.RaidID.String() ||
		event.GetPlayerId() != 10 || event.GetClanId() != 1 || event.GetDamage() != 555 ||
		!event.GetIsCrit() || !event.GetCreatedAt().AsTime().Equal(hit.CreatedAt) {
		t.Fatalf("unexpected event: %v", &event)
	}
}

// Все удары одного рейда идут в одну партицию, разные рейды раскладываются по разным.
func TestProducer_SameRaidSamePartition(t *testing.T) {
	t.Parallel()

	brokers := startCluster(t)
	producer := newProducer(t, brokers)
	ctx := context.Background()

	const (
		raids       = 30
		hitsPerRaid = 5
	)
	for range raids {
		raidID := uuid.New()
		for range hitsPerRaid {
			if err := producer.Publish(ctx, newHit(raidID)); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
	}

	partitionOf := map[string]int32{}
	used := map[int32]bool{}
	for _, rec := range consume(t, brokers, raids*hitsPerRaid) {
		raid := string(rec.Key)
		if p, ok := partitionOf[raid]; ok && p != rec.Partition {
			t.Fatalf("raid %s written to partitions %d and %d", raid, p, rec.Partition)
		}
		partitionOf[raid] = rec.Partition
		used[rec.Partition] = true
	}
	if len(used) < 2 {
		t.Fatalf("all raids landed in %d partition(s), expected a spread", len(used))
	}
}

func TestProducer_ConcurrentPublish(t *testing.T) {
	t.Parallel()

	brokers := startCluster(t)
	producer := newProducer(t, brokers)

	const n = 200
	errs := make(chan error, n)
	raidID := uuid.New()
	for range n {
		go func() { errs <- producer.Publish(context.Background(), newHit(raidID)) }()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if got := len(consume(t, brokers, n)); got != n {
		t.Fatalf("consumed %d records, want %d", got, n)
	}
}

// Брокер недоступен: дедлайн клиента даёт DeadlineExceeded, а не зависание.
func TestProducer_ClientDeadlineWhenBrokerDown(t *testing.T) {
	t.Parallel()

	producer := newProducer(t, []string{"127.0.0.1:1"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	start := time.Now()
	err := producer.Publish(ctx, newHit(uuid.New()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Publish hung for %v", elapsed)
	}
}

// Брокер недоступен и дедлайна нет: отправка всё равно заканчивается ошибкой по таймауту доставки.
func TestProducer_DeliveryTimeoutWhenBrokerDown(t *testing.T) {
	t.Parallel()

	producer := newProducer(t, []string{"127.0.0.1:1"}, kgo.RecordDeliveryTimeout(time.Second))

	err := producer.Publish(context.Background(), newHit(uuid.New()))
	if err == nil {
		t.Fatal("Publish must fail when the broker is unreachable")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v: a broker failure must not look like a client deadline", err)
	}
}

func TestProducer_Ping(t *testing.T) {
	t.Parallel()

	producer := newProducer(t, startCluster(t))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestNewProducer_Validation(t *testing.T) {
	t.Parallel()

	if _, err := NewProducer(Config{Topic: testTopic}); err == nil {
		t.Fatal("no brokers must be an error")
	}
	if _, err := NewProducer(Config{Brokers: []string{"a:1"}}); err == nil {
		t.Fatal("no topic must be an error")
	}
}

// Брокер недоступен, у клиента дедлайн 2 с: продюсер сдаётся раньше (по таймауту доставки) и возвращает
// сбой брокера, а не дедлайн клиента — иначе Attack отвечал бы DEADLINE_EXCEEDED вместо UNAVAILABLE.
func TestProducer_BrokerDownBeatsClientDeadline(t *testing.T) {
	t.Parallel()

	producer := newProducer(t, []string{"127.0.0.1:1"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := producer.Publish(ctx, newHit(uuid.New()))
	if err == nil {
		t.Fatal("Publish must fail when the broker is unreachable")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v: a broker failure must not look like a client deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("Publish took %v, want about deliveryTimeout (%v)", elapsed, deliveryTimeout)
	}
}

// Подключение к брокеру зависает (например, DNS остановленного контейнера отвечает десятки секунд):
// delivery timeout franz-go в это время не срабатывает, но Publish обязан вернуться сам за deliveryTimeout
// с ошибкой очереди, а не с дедлайном клиента — иначе предохранитель десятки секунд не видит сбоя.
func TestProducer_HangingDialBeatsClientDeadline(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hang := kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("dial: no such host")
	})

	producer := newProducer(t, []string{"kafka:9092"}, hang)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := producer.Publish(ctx, newHit(uuid.New()))
	if err == nil {
		t.Fatal("Publish must fail when the broker cannot be reached")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v: a connection hang must not look like a client deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("Publish took %v, want about deliveryTimeout (%v)", elapsed, deliveryTimeout)
	}
}

// Клиент kafka застрял и не реагирует на контекст (так вёл себя franz-go при остановленном брокере):
// Publish обязан вернуться сам за deliveryTimeout с ошибкой очереди.
func TestProducer_StuckClientIsBoundedByDeliveryTimeout(t *testing.T) {
	t.Parallel()

	producer := newProducer(t, startCluster(t))
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	producer.produce = func(context.Context, *kgo.Record) error {
		<-release // игнорирует контекст
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := producer.Publish(ctx, newHit(uuid.New()))
	if !errors.Is(err, errPublishTimeout) {
		t.Fatalf("err = %v, want errPublishTimeout", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v: a queue failure must not look like a client deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 2*deliveryTimeout {
		t.Fatalf("Publish took %v, want about %v", elapsed, deliveryTimeout)
	}
}

// Клиентский дедлайн короче deliveryTimeout остаётся дедлайном клиента.
func TestProducer_ShortClientDeadlineStaysClientError(t *testing.T) {
	t.Parallel()

	producer := newProducer(t, startCluster(t))
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	producer.produce = func(context.Context, *kgo.Record) error {
		<-release
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := producer.Publish(ctx, newHit(uuid.New()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the client deadline", err)
	}
}
