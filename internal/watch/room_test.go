package watch

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

func TestRoom_FirstUpdateIsImmediate(t *testing.T) {
	t.Parallel()
	r := newRoom(250 * time.Millisecond)

	out, wait := r.accept(t0, hpUpdate(10))
	if len(out) != 1 || wait != 0 {
		t.Fatalf("out=%d wait=%v, want immediate delivery", len(out), wait)
	}
}

func TestRoom_ThrottlesAndCollapsesToLast(t *testing.T) {
	t.Parallel()
	r := newRoom(250 * time.Millisecond)
	r.accept(t0, hpUpdate(10))

	out, wait := r.accept(t0.Add(50*time.Millisecond), hpUpdate(9))
	if len(out) != 0 || wait != 200*time.Millisecond {
		t.Fatalf("out=%d wait=%v, want arm timer for 200ms", len(out), wait)
	}
	// Пока таймер взведён, новые обновления только копятся и таймер не пере-взводится.
	out, wait = r.accept(t0.Add(100*time.Millisecond), hpUpdate(8))
	if len(out) != 0 || wait != 0 {
		t.Fatalf("out=%d wait=%v, want accumulate only", len(out), wait)
	}

	out = r.flush(t0.Add(250 * time.Millisecond))
	if len(out) != 1 || out[0].GetHpLeft() != 8 {
		t.Fatalf("flush = %v, want the last hp_left=8", out)
	}
	if got := r.flush(t0.Add(300 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("second flush = %v, want nothing", got)
	}
}

func TestRoom_KindsAreCollapsedIndependently(t *testing.T) {
	t.Parallel()
	r := newRoom(time.Second)
	r.accept(t0, hpUpdate(10))
	r.accept(t0.Add(10*time.Millisecond), hpUpdate(9))
	r.accept(t0.Add(20*time.Millisecond), leaderboardUpdate())

	out := r.flush(t0.Add(time.Second))
	if len(out) != 2 || out[0].GetHpLeft() != 9 || out[1].GetLeaderboard() == nil {
		t.Fatalf("flush = %v, want hp_left=9 and leaderboard", out)
	}
}

func TestRoom_ImmediateAgainAfterInterval(t *testing.T) {
	t.Parallel()
	r := newRoom(250 * time.Millisecond)
	r.accept(t0, hpUpdate(10))

	out, wait := r.accept(t0.Add(250*time.Millisecond), hpUpdate(9))
	if len(out) != 1 || wait != 0 {
		t.Fatalf("out=%d wait=%v, want immediate delivery", len(out), wait)
	}
}

func TestRoom_FinishedIsNotThrottledAndDropsPending(t *testing.T) {
	t.Parallel()
	r := newRoom(time.Second)
	r.accept(t0, hpUpdate(10))
	r.accept(t0.Add(10*time.Millisecond), hpUpdate(0))

	out, wait := r.accept(t0.Add(20*time.Millisecond), finishedUpdate())
	if len(out) != 1 || out[0].GetFinished() == nil || wait != 0 {
		t.Fatalf("out=%v wait=%v, want only finished", out, wait)
	}
	if got := r.flush(t0.Add(time.Second)); len(got) != 0 {
		t.Fatalf("flush after finished = %v, want nothing", got)
	}
	if out, _ := r.accept(t0.Add(2*time.Second), hpUpdate(5)); len(out) != 0 {
		t.Fatalf("update after finished = %v, want ignored", out)
	}
}

func TestRoom_NegativeIntervalDisablesThrottling(t *testing.T) {
	t.Parallel()
	r := newRoom(-1)
	for i := range 5 {
		out, wait := r.accept(t0, hpUpdate(int64(i)))
		if len(out) != 1 || wait != 0 {
			t.Fatalf("update %d: out=%d wait=%v, want immediate delivery", i, len(out), wait)
		}
	}
}
