package main

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

func TestPercentile(t *testing.T) {
	t.Parallel()

	sorted := make([]time.Duration, 0, 100)
	for i := 1; i <= 100; i++ {
		sorted = append(sorted, time.Duration(i)*time.Millisecond)
	}
	tests := []struct {
		p    float64
		want time.Duration
	}{{0, time.Millisecond}, {50, 50 * time.Millisecond}, {99, 99 * time.Millisecond}, {100, 100 * time.Millisecond}}
	for _, tc := range tests {
		if got := percentile(sorted, tc.p); got != tc.want {
			t.Errorf("percentile(%v) = %v, want %v", tc.p, got, tc.want)
		}
	}
	if got := percentile(nil, 99); got != 0 {
		t.Errorf("percentile of empty = %v, want 0", got)
	}
}

func TestStatsMerge(t *testing.T) {
	t.Parallel()

	a, b := newStats(), newStats()
	a.record(codes.OK, time.Millisecond)
	a.accepted = 1
	b.record(codes.OK, 2*time.Millisecond)
	b.record(codes.Unavailable, 3*time.Millisecond)
	b.retries = 1

	a.merge(b)
	if len(a.latencies) != 3 || a.codes[codes.OK] != 2 || a.codes[codes.Unavailable] != 1 ||
		a.accepted != 1 || a.retries != 1 {
		t.Fatalf("merged = %+v", a)
	}
}
