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
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func TestLoadConfig(t *testing.T) {
	t.Setenv("APP_HOST", "")
	t.Setenv("APP_PORT", "")
	t.Setenv("GRPC_REFLECTION", "")

	tests := []struct {
		name           string
		env            map[string]string
		wantAddr       string
		wantReflection bool
		wantErr        bool
	}{
		{name: "defaults", env: nil, wantAddr: ":9090", wantReflection: false},
		{name: "different host", env: map[string]string{"APP_HOST": "example.com"}, wantAddr: "example.com:9090"},
		{name: "custom port", env: map[string]string{"APP_PORT": "8080"}, wantAddr: ":8080"},
		{name: "port not a number", env: map[string]string{"APP_PORT": "abc"}, wantErr: true},
		{name: "port too small", env: map[string]string{"APP_PORT": "0"}, wantErr: true},
		{name: "port too big", env: map[string]string{"APP_PORT": "70000"}, wantErr: true},
		{name: "wrong GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "70000"}, wantErr: true},
		{name: "enable by 1 GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "1"}, wantReflection: true},
		{name: "enable by true GRPC_REFLECTION", env: map[string]string{"GRPC_REFLECTION": "true"}, wantReflection: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			cfg, err := loadConfig()
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadConfig() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if tc.wantAddr != "" && cfg.gRPCAddr != tc.wantAddr {
				t.Fatalf("loadConfig() wrog parsed addres got = %v, expected %v", cfg.gRPCAddr, tc.wantAddr)
			}

			if cfg.gRPCReflection != tc.wantReflection {
				t.Fatalf("loadConfig() wrog reflection got = %v, expected %v", cfg.gRPCReflection, tc.wantReflection)
			}
		})
	}
}

func startTestServer(t *testing.T, cfg *Config) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := newGRPCServer(newRaidService(), cfg)
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

func TestRaidServiceIsUnimplemented(t *testing.T) {
	t.Parallel()

	conn := startTestServer(t, &Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := raidbossV1.NewRaidServiceClient(conn).GetRaid(ctx, &raidbossV1.GetRaidRequest{})
	if got := status.Code(err); got != codes.Unimplemented {
		t.Fatalf("GetRaid code = %v, want %v (err: %v)", got, codes.Unimplemented, err)
	}
}

func TestCheckDbStatus(t *testing.T) {
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
