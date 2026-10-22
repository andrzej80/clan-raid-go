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

func TestUnaryAuth(t *testing.T) {
	t.Parallel()

	const (
		attack = raidbossV1.RaidService_Attack_FullMethodName
		get    = raidbossV1.RaidService_GetRaid_FullMethodName
		health = "/grpc.health.v1.Health/Check"
	)
	tests := []struct {
		name   string
		method string
		header []string // nil — заголовка нет
		want   codes.Code
		wantID int64
	}{
		{"attack with player", attack, []string{"42"}, codes.OK, 42},
		{"attack without header", attack, nil, codes.Unauthenticated, 0},
		{"attack empty header", attack, []string{""}, codes.Unauthenticated, 0},
		{"attack not a number", attack, []string{"abc"}, codes.Unauthenticated, 0},
		{"attack zero", attack, []string{"0"}, codes.Unauthenticated, 0},
		{"attack negative", attack, []string{"-1"}, codes.Unauthenticated, 0},
		{"get raid without header", get, nil, codes.Unauthenticated, 0},
		{"get raid with player", get, []string{"7"}, codes.OK, 7},
		{"health without header", health, nil, codes.OK, 0},
		{"health ignores bad header", health, []string{"abc"}, codes.OK, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tc.header != nil {
				ctx = metadata.NewIncomingContext(ctx, metadata.MD{PlayerIDHeader: tc.header})
			}
			var gotID int64
			handler := func(ctx context.Context, _ any) (any, error) {
				gotID, _ = PlayerIDFromContext(ctx)
				return nil, nil
			}

			auth := NewAuth("grpc.health.v1.Health")
			_, err := auth.Unary()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: tc.method}, handler)
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v (err: %v)", got, tc.want, err)
			}
			if gotID != tc.wantID {
				t.Fatalf("player id = %d, want %d", gotID, tc.wantID)
			}
		})
	}
}
