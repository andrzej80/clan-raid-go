package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func startTestServer(t *testing.T, cfg *config.Config) *grpc.ClientConn {
	t.Helper()

	return startTestServerWith(t, cfg, grpcapi.NewRaidService(nil, nil))
}

// allowAll — ограничитель, который никогда не отказывает: большинству тестов лимит не важен.
type allowAll struct{}

func (allowAll) Allow(context.Context, int64) (bool, time.Duration, error) { return true, 0, nil }

func startTestServerWith(t *testing.T, cfg *config.Config, svc raidbossV1.RaidServiceServer) *grpc.ClientConn {
	t.Helper()

	return startTestServerWithLimiter(t, cfg, svc, allowAll{})
}

func startTestServerWithLimiter(
	t *testing.T,
	cfg *config.Config,
	svc raidbossV1.RaidServiceServer,
	limiter interceptors.Limiter,
) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := newGRPCServer(svc, cfg, limiter)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func TestAttackRequiresPlayerID(t *testing.T) {
	t.Parallel()

	conn := startTestServer(t, &config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := raidbossV1.NewRaidServiceClient(conn).Attack(ctx, &raidbossV1.AttackRequest{})
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("Attack code = %v, want %v (err: %v)", got, codes.Unauthenticated, err)
	}
}

func TestCheckDbStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		startStatus healthpb.HealthCheckResponse_ServingStatus
		finalStatus healthpb.HealthCheckResponse_ServingStatus
		pingError   error
	}{
		{name: "turn on", startStatus: healthpb.HealthCheckResponse_NOT_SERVING, finalStatus: healthpb.HealthCheckResponse_SERVING, pingError: nil},
		{name: "turn off", startStatus: healthpb.HealthCheckResponse_SERVING, finalStatus: healthpb.HealthCheckResponse_NOT_SERVING, pingError: errors.New("ping error")},
		{name: "no change enabled", startStatus: healthpb.HealthCheckResponse_SERVING, finalStatus: healthpb.HealthCheckResponse_SERVING, pingError: nil},
		{name: "no change bisabled", startStatus: healthpb.HealthCheckResponse_NOT_SERVING, finalStatus: healthpb.HealthCheckResponse_NOT_SERVING, pingError: errors.New("ping error")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hs := health.NewServer()
			hs.SetServingStatus("", tc.startStatus)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			gotStatus := checkDbStatus(
				ctx,
				fakePinger{err: tc.pingError},
				tc.startStatus,
				hs,
			)

			if gotStatus != tc.finalStatus {
				t.Fatalf("health check failed resived status %v expected %v", gotStatus, tc.finalStatus)
			}
			status, err := hs.Check(ctx, &healthpb.HealthCheckRequest{})
			if err != nil {
				t.Fatalf("health check failed %v", err)
			}
			if status.Status != tc.finalStatus {
				t.Fatalf("health check failed resived status %v", status.Status)
			}
		})
	}
}
