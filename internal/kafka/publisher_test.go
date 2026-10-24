package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

func TestNewPublisherRequiresBrokers(t *testing.T) {
	t.Parallel()

	if _, err := NewPublisher(nil); err == nil {
		t.Fatal("NewPublisher without brokers succeeded, want error")
	}
}

func TestPublisher_PublishBatchRoutesByRecordTopic(t *testing.T) {
	t.Parallel()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "raid.events", "rewards.granted"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	brokers := cluster.ListenAddrs()

	pub, err := NewPublisher(brokers)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pub.Close(ctx)
	})

	msgs := []domain.OutboxMessage{
		{
			Topic: "raid.events", Key: "raid-1", Payload: []byte("a"),
			Headers: map[string]string{"type": "RaidDefeated", "b": "2"},
		},
		{Topic: "rewards.granted", Key: "42", Payload: []byte("b")},
	}
	if err := pub.PublishBatch(context.Background(), msgs); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}

	got := consumeTopics(t, brokers, 2, "raid.events", "rewards.granted")
	byTopic := map[string]*kgo.Record{}
	for _, r := range got {
		byTopic[r.Topic] = r
	}

	ev := byTopic["raid.events"]
	if ev == nil || string(ev.Key) != "raid-1" || string(ev.Value) != "a" {
		t.Fatalf("raid.events record = %+v", ev)
	}
	if len(ev.Headers) != 2 || ev.Headers[0].Key != "b" || ev.Headers[1].Key != "type" ||
		string(ev.Headers[1].Value) != "RaidDefeated" {
		t.Fatalf("headers = %+v, want sorted [b type]", ev.Headers)
	}
	rw := byTopic["rewards.granted"]
	if rw == nil || string(rw.Key) != "42" || len(rw.Headers) != 0 {
		t.Fatalf("rewards.granted record = %+v", rw)
	}
}

func TestPublisher_PublishBatchFailsWhenBrokerIsDown(t *testing.T) {
	t.Parallel()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "raid.events"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	pub, err := NewPublisher(cluster.ListenAddrs(), kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close(context.Background()) })
	cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = pub.PublishBatch(ctx, []domain.OutboxMessage{{Topic: "raid.events", Key: "k", Payload: []byte("x")}})
	if err == nil {
		t.Fatal("PublishBatch succeeded with the broker down, want error")
	}
}

// consumeTopics читает из топиков ровно want записей.
func consumeTopics(t *testing.T, brokers []string, want int, topics ...string) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topics...),
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
