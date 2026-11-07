package kafka

import (
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testTopicsConfig() TopicsConfig {
	return TopicsConfig{
		HitsTopic: "raid.hits", HitsDLQTopic: "raid.hits.dlq",
		EventsTopic: "raid.events", EventsDLQTopic: "raid.events.dlq",
		RewardsTopic: "rewards.granted", ReplicationFactor: 1,
	}
}

func newTopicAdmin(t *testing.T, opts ...kfake.Opt) (*TopicAdmin, *kfake.Cluster) {
	t.Helper()
	cluster, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(cl.Close)
	return NewTopicAdmin(cl), cluster
}

func TestMinInSyncReplicas(t *testing.T) {
	t.Parallel()
	for rf, want := range map[int16]int16{1: 1, 2: 1, 3: 2, 5: 2} {
		if got := MinInSyncReplicas(rf); got != want {
			t.Errorf("MinInSyncReplicas(%d) = %d, want %d", rf, got, want)
		}
	}
}

func TestTopics_ReplicationAndInSync(t *testing.T) {
	t.Parallel()
	cfg := testTopicsConfig()
	cfg.ReplicationFactor = 3
	specs := Topics(cfg)
	if len(specs) != 5 {
		t.Fatalf("want 5 topics, got %d", len(specs))
	}
	for _, s := range specs {
		if s.ReplicationFactor != 3 || s.Configs["min.insync.replicas"] != "2" {
			t.Errorf("%s: rf %d, min.insync %q; want 3 and 2", s.Name, s.ReplicationFactor, s.Configs["min.insync.replicas"])
		}
	}
	if specs[0].Name != "raid.hits" || specs[0].Partitions != 12 {
		t.Errorf("raid.hits spec = %+v", specs[0])
	}
}

func TestTopicAdmin_ApplyCreatesMissingAndIsIdempotent(t *testing.T) {
	t.Parallel()
	adm, _ := newTopicAdmin(t)
	specs := Topics(testTopicsConfig())

	applied, remaining, err := adm.Apply(t.Context(), specs)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(applied) != len(specs) || len(remaining) != 0 {
		t.Fatalf("first apply: applied %v, remaining %v; want %d created", applied, remaining, len(specs))
	}

	drifts, err := adm.Diff(t.Context(), specs)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(drifts) != 0 {
		t.Fatalf("after apply cluster must match, got %v", drifts)
	}

	applied, _, err = adm.Apply(t.Context(), specs)
	if err != nil || len(applied) != 0 {
		t.Fatalf("second apply: applied %v, err %v; want nothing to do", applied, err)
	}
}

func TestTopicAdmin_ConfigDriftIsFixed(t *testing.T) {
	t.Parallel()
	adm, cluster := newTopicAdmin(t)
	specs := Topics(testTopicsConfig())[:1] // raid.hits
	if err := cluster.CreateTopic("raid.hits", 12, map[string]string{"retention.ms": "1000"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	drifts, err := adm.Diff(t.Context(), specs)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	idx := slices.IndexFunc(drifts, func(d Drift) bool { return d.Kind == DriftConfig && d.Key == "retention.ms" })
	if idx < 0 || drifts[idx].Got != "1000" || !drifts[idx].Fixable {
		t.Fatalf("want fixable retention.ms drift from 1000, got %v", drifts)
	}

	if _, remaining, err := adm.Apply(t.Context(), specs); err != nil || len(remaining) != 0 {
		t.Fatalf("Apply: remaining %v, err %v", remaining, err)
	}
	if drifts, err := adm.Diff(t.Context(), specs); err != nil || len(drifts) != 0 {
		t.Fatalf("after apply: drifts %v, err %v", drifts, err)
	}
}

func TestTopicAdmin_PartitionDriftIsNotApplied(t *testing.T) {
	t.Parallel()
	adm, cluster := newTopicAdmin(t)
	specs := Topics(testTopicsConfig())[:1] // raid.hits, 12 партиций
	if err := cluster.CreateTopic("raid.hits", 3, nil); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, remaining, err := adm.Apply(t.Context(), specs)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Kind != DriftPartitions || remaining[0].Got != "3" || remaining[0].Fixable {
		t.Fatalf("want unfixable partitions drift 3 -> 12, got %v", remaining)
	}
}
