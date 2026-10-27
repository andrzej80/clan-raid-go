package main

import (
	"testing"
	"time"
)

func TestCurrentRate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		elapsed time.Duration
		ramp    time.Duration
		want    float64
	}{
		{"start of ramp uses floor", 0, 10 * time.Second, 1},
		{"half of ramp", 5 * time.Second, 10 * time.Second, 50},
		{"after ramp", 20 * time.Second, 10 * time.Second, 100},
		{"no ramp", 0, 0, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := currentRate(tt.elapsed, 100, tt.ramp); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickTargetSkipsDead(t *testing.T) {
	t.Parallel()

	targets := []*target{{}, {}, {}}
	targets[1].dead.Store(true)

	idx := 0
	got := make([]*target, 0, 4)
	for range 4 {
		tg, ok := pickTarget(targets, &idx)
		if !ok {
			t.Fatal("expected a live target")
		}
		got = append(got, tg)
	}
	want := []*target{targets[0], targets[2], targets[0], targets[2]}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pick %d: wrong target", i)
		}
	}

	targets[0].dead.Store(true)
	targets[2].dead.Store(true)
	if _, ok := pickTarget(targets, &idx); ok {
		t.Fatal("expected no live targets")
	}
}
