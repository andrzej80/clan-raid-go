package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
)

func startTopicsCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	t.Setenv("KAFKA_BROKERS", strings.Join(cluster.ListenAddrs(), ","))
	t.Setenv("KAFKA_REPLICATION_FACTOR", "1")
	return cluster
}

func TestTopics_ApplyThenDiffMatches(t *testing.T) { //nolint:paralleltest // t.Setenv: KAFKA_BROKERS
	startTopicsCluster(t)

	var out bytes.Buffer
	if err := run(t.Context(), []string{"topics", "apply"}, &out); err != nil {
		t.Fatalf("apply: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "applied: raid.hits: missing") {
		t.Fatalf("apply output must list created topics:\n%s", out.String())
	}

	out.Reset()
	if err := run(t.Context(), []string{"topics", "diff"}, &out); err != nil {
		t.Fatalf("diff after apply: %v\n%s", err, out.String())
	}
}

func TestTopics_DiffReportsDriftAndApplyRefusesPartitions(t *testing.T) { //nolint:paralleltest // t.Setenv: KAFKA_BROKERS
	cluster := startTopicsCluster(t)
	if err := cluster.CreateTopic("raid.hits", 3, nil); err != nil {
		t.Fatalf("create: %v", err)
	}

	var out bytes.Buffer
	err := run(t.Context(), []string{"topics", "diff"}, &out)
	if !errors.Is(err, errTopicsDrift) || !strings.Contains(out.String(), "raid.hits: partitions = 3, want 12") {
		t.Fatalf("diff: err %v, output:\n%s", err, out.String())
	}

	out.Reset()
	err = run(t.Context(), []string{"topics", "apply", "--wait", "0s"}, &out)
	if !errors.Is(err, errTopicsDrift) || !strings.Contains(out.String(), "not applied: raid.hits: partitions") {
		t.Fatalf("apply: err %v, output:\n%s", err, out.String())
	}
}

func TestTopics_RequiresBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	if err := run(t.Context(), []string{"topics", "diff"}, &bytes.Buffer{}); err == nil {
		t.Fatal("want error without KAFKA_BROKERS")
	}
}
