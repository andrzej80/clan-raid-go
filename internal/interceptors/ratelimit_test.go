package interceptors

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const limitedMethod = "/raidboss.v1.RaidService/Attack"

type fakeLimiter struct {
	ok         bool
	retryAfter time.Duration
	err        error
	gotPlayer  int64
	calls      int
}

func (f *fakeLimiter) Allow(_ context.Context, playerID int64) (bool, time.Duration, error) {
	f.calls++
	f.gotPlayer = playerID
	return f.ok, f.retryAfter, f.err
}

func callRateLimit(ctx context.Context, l Limiter, method string) (handled bool, err error) {
	interceptor := UnaryRateLimit(l, limitedMethod)
	_, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method},
		func(context.Context, any) (any, error) {
			handled = true
			return nil, nil
		})
	return handled, err
}

func ctxWithPlayer(id int64) context.Context {
	return context.WithValue(context.Background(), playerIDKey{}, id)
}

func TestRateLimit_AllowedPassesThrough(t *testing.T) {
	t.Parallel()
	l := &fakeLimiter{ok: true}

	handled, err := callRateLimit(ctxWithPlayer(42), l, limitedMethod)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want handled/nil", handled, err)
	}
	if l.gotPlayer != 42 {
		t.Fatalf("limiter got player %d, want 42", l.gotPlayer)
	}
}

func TestRateLimit_RejectedHasRetryInfo(t *testing.T) {
	t.Parallel()
	l := &fakeLimiter{ok: false, retryAfter: 150*time.Millisecond + 300*time.Microsecond}

	handled, err := callRateLimit(ctxWithPlayer(1), l, limitedMethod)
	if handled {
		t.Fatal("handler must not run when the limit is exceeded")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", got)
	}

	var info *errdetails.RetryInfo
	for _, d := range status.Convert(err).Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			info = ri
		}
	}
	if info == nil {
		t.Fatal("RetryInfo detail is missing")
	}
	if got := info.GetRetryDelay().AsDuration(); got != 151*time.Millisecond {
		t.Fatalf("retry delay = %v, want 151ms (rounded up to a millisecond)", got)
	}
}

func TestRateLimit_OtherMethodsAreNotLimited(t *testing.T) {
	t.Parallel()
	l := &fakeLimiter{ok: false}

	handled, err := callRateLimit(ctxWithPlayer(1), l, "/raidboss.v1.RaidService/GetRaid")
	if err != nil || !handled || l.calls != 0 {
		t.Fatalf("handled=%v err=%v limiter calls=%d, want handled without touching the limiter", handled, err, l.calls)
	}
}

func TestRateLimit_LimiterFailureFailsOpen(t *testing.T) {
	t.Parallel()
	l := &fakeLimiter{err: errors.New("redis down")}

	handled, err := callRateLimit(ctxWithPlayer(1), l, limitedMethod)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, a broken limiter must not block requests", handled, err)
	}
}

func TestRateLimit_NoPlayerInContext(t *testing.T) {
	t.Parallel()
	l := &fakeLimiter{ok: true}

	handled, err := callRateLimit(context.Background(), l, limitedMethod)
	if handled || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("handled=%v code=%v, want rejected as Unauthenticated", handled, status.Code(err))
	}
}
