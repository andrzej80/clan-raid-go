package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// fakeStream — минимальный grpc.ServerStream: интерсепторам нужен только Context().
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context //nolint:containedctx // тестовая заглушка стрима
}

func (s fakeStream) Context() context.Context { return s.ctx }

func TestStreamAuth(t *testing.T) {
	t.Parallel()

	const (
		watch  = raidbossV1.RaidService_WatchRaid_FullMethodName
		health = "/grpc.health.v1.Health/Watch"
	)
	tests := []struct {
		name   string
		method string
		header []string // nil — заголовка нет
		want   codes.Code
		wantID int64
	}{
		{"watch with player", watch, []string{"42"}, codes.OK, 42},
		{"watch without header", watch, nil, codes.Unauthenticated, 0},
		{"watch bad header", watch, []string{"abc"}, codes.Unauthenticated, 0},
		{"public stream without header", health, nil, codes.OK, 0},
		{"public stream ignores bad header", health, []string{"abc"}, codes.OK, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tc.header != nil {
				ctx = metadata.NewIncomingContext(ctx, metadata.MD{PlayerIDHeader: tc.header})
			}

			var (
				called bool
				gotID  int64
			)
			handler := func(_ any, ss grpc.ServerStream) error {
				called = true
				gotID, _ = PlayerIDFromContext(ss.Context())
				return nil
			}

			err := NewAuth("grpc.health.v1.Health").Stream()(
				nil, fakeStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: tc.method}, handler)

			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v (err: %v)", got, tc.want, err)
			}
			if wantCalled := tc.want == codes.OK; called != wantCalled {
				t.Fatalf("handler called = %v, want %v", called, wantCalled)
			}
			if gotID != tc.wantID {
				t.Fatalf("player id in stream context = %d, want %d", gotID, tc.wantID)
			}
		})
	}
}
