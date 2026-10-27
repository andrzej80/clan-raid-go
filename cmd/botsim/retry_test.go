package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

func TestRetryBudget(t *testing.T) {
	t.Parallel()

	b := &retryBudget{ratio: 0.5}
	if b.withdraw() {
		t.Fatal("an empty budget must deny retries")
	}
	b.deposit()
	b.deposit() // 2 × 0.5 = 1 токен
	if !b.withdraw() {
		t.Fatal("one token must allow one retry")
	}
	if b.withdraw() {
		t.Fatal("the token is spent")
	}
}

func TestRetryBudgetCapsBurst(t *testing.T) {
	t.Parallel()

	b := &retryBudget{ratio: 1}
	for range 1000 {
		b.deposit()
	}
	var allowed int
	for b.withdraw() {
		allowed++
	}
	if allowed != retryBudgetBurst {
		t.Fatalf("allowed %d retries, want the burst cap %d", allowed, retryBudgetBurst)
	}
}

func TestRetryBudgetDisabled(t *testing.T) {
	t.Parallel()

	b := newRetryBudget(-1)
	b.deposit()
	for range 1000 {
		if !b.withdraw() {
			t.Fatal("a negative ratio must not limit retries")
		}
	}
}

func TestRetryDelayJitterAndGrowth(t *testing.T) {
	t.Parallel()

	for attempt := range 4 {
		base := retryBackoff << attempt
		for range 200 {
			d := retryDelay(attempt, nil)
			if d < base/2 || d > base {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, base/2, base)
			}
		}
	}
	if d := retryDelay(30, nil); d > retryBackoffMax {
		t.Fatalf("delay %v exceeds the cap %v", d, retryBackoffMax)
	}
}

func TestRetryDelayHonorsRetryInfo(t *testing.T) {
	t.Parallel()

	st, err := status.New(codes.ResourceExhausted, "rate limit").
		WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if d := retryDelay(0, st.Err()); d < 2*time.Second {
		t.Fatalf("delay %v is shorter than the server's RetryInfo", d)
	}

	huge, err := status.New(codes.ResourceExhausted, "rate limit").
		WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if d := retryDelay(0, huge.Err()); d > retryBackoffMax {
		t.Fatalf("delay %v: a server hint must not stall a bot longer than %v", d, retryBackoffMax)
	}
}

type fakeAttack struct {
	raidbossV1.UnimplementedRaidServiceServer
	calls atomic.Int32
	err   error
}

func (f *fakeAttack) Attack(context.Context, *raidbossV1.AttackRequest) (*raidbossV1.AttackResponse, error) {
	f.calls.Add(1)
	return nil, f.err
}

func TestAttackStopsRetryingWhenBudgetIsExhausted(t *testing.T) {
	t.Parallel()

	srv := &fakeAttack{err: status.Error(codes.Unavailable, "queue down")}
	client := newWatchClient(t, srv)
	st := newStats(time.Now())

	code := attack(context.Background(), client, player{id: 1}, "raid", "key", st, &retryBudget{ratio: 0.1})
	if code != codes.Unavailable {
		t.Fatalf("code = %v", code)
	}
	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1: an empty budget must not allow a retry", got)
	}
	if r := st.report(time.Second); r.RetriesDenied != 1 || r.Retries != 0 {
		t.Fatalf("retries=%d denied=%d, want 0 and 1", r.Retries, r.RetriesDenied)
	}
}

func TestAttackDoesNotRetryNonRetryableCodes(t *testing.T) {
	t.Parallel()

	srv := &fakeAttack{err: status.Error(codes.FailedPrecondition, "raid is over")}
	client := newWatchClient(t, srv)

	attack(context.Background(), client, player{id: 1}, "raid", "key", newStats(time.Now()), nil)
	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
