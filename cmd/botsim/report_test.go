package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

func TestReportPercentiles(t *testing.T) {
	t.Parallel()

	s := newStats(time.Now())
	for i := 1; i <= 1000; i++ {
		s.record(codes.OK, time.Duration(i)*time.Millisecond)
	}
	r := s.report(10 * time.Second)

	if r.RPCTotal != 1000 || r.RPS != 100 {
		t.Fatalf("total=%d rps=%v", r.RPCTotal, r.RPS)
	}
	// Гистограмма с 2 значащими цифрами даёт ~1% погрешности.
	for name, tc := range map[string]struct{ got, want float64 }{
		"p50": {r.LatencyMs.P50, 500}, "p95": {r.LatencyMs.P95, 950},
		"p99": {r.LatencyMs.P99, 990}, "p99.9": {r.LatencyMs.P999, 999}, "max": {r.LatencyMs.Max, 1000},
	} {
		if math.Abs(tc.got-tc.want)/tc.want > 0.02 {
			t.Errorf("%s = %v, want ~%v", name, tc.got, tc.want)
		}
	}
}

func TestReportEmpty(t *testing.T) {
	t.Parallel()

	r := newStats(time.Now()).report(time.Second)
	if r.RPCTotal != 0 || r.LatencyMs.P99 != 0 {
		t.Fatalf("report of empty stats = %+v", r)
	}
	_ = r.format() // не должен паниковать на пустых данных
}

func TestRecordClampsOutOfRange(t *testing.T) {
	t.Parallel()

	s := newStats(time.Now())
	s.record(codes.OK, 0)
	s.record(codes.DeadlineExceeded, 2*time.Hour)
	if got := s.report(time.Second).RPCTotal; got != 2 {
		t.Fatalf("total = %d, want 2: out-of-range values must not be lost", got)
	}
}

func TestStatsMerge(t *testing.T) {
	t.Parallel()

	epoch := time.Now().Add(-2 * time.Second) // записи попадают во вторую секунду прогона
	a, b := newStats(epoch), newStats(epoch)
	a.record(codes.OK, time.Millisecond)
	a.markAccepted()
	b.record(codes.OK, 2*time.Millisecond)
	b.record(codes.Unavailable, 3*time.Millisecond)
	b.markRetry()
	b.markDropped()

	a.merge(b)
	r := a.report(3 * time.Second)
	if r.RPCTotal != 3 || r.Codes["OK"] != 2 || r.Codes["Unavailable"] != 1 ||
		r.OK != 1 || r.Retries != 1 || r.Dropped != 1 {
		t.Fatalf("merged = %+v", r)
	}
	if len(r.RPSPerSecond) != 3 || r.RPSPerSecond[2] != 3 {
		t.Fatalf("rps per second = %v, want 3 requests in bucket 2", r.RPSPerSecond)
	}
}

func TestReportJSON(t *testing.T) {
	t.Parallel()

	s := newStats(time.Now())
	s.record(codes.OK, 5*time.Millisecond)
	data, err := s.report(time.Second).json()
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.RPCTotal != 1 || got.Codes["OK"] != 1 {
		t.Fatalf("round trip = %+v", got)
	}
}
