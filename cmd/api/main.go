package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
)

const (
	shutdownTimeout     = 10 * time.Second
	healthCheckInterval = 2 * time.Second
	dbPingTimeout       = 500 * time.Millisecond
)

type pinger interface {
	Ping(ctx context.Context) error
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}

	slog.Info("gRPC server stopped", "port", cfg.GRPCAddr, "isDevMode", cfg.GRPCReflection)
}

func run(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx)
	if err != nil {
		return err
	}
	tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
		if err := shutdownTelemetry(tctx); err != nil {
			slog.Warn("Telemetry shutdown error")
		}
	}()

	dbPool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	raidRepo := pg.NewRaidRepo(dbPool)
	service := grpcapi.NewRaidService(raid.NewService(raidRepo))

	srv, lis, err := initGRPCServer(ctx, cfg, service)
	if err != nil {
		return err
	}

	hs := health.NewServer()
	defer hs.Shutdown()
	healthpb.RegisterHealthServer(srv, hs)

	go watchHealth(ctx, dbPool, hs)

	errCh := make(chan error, 1)

	go func() {
		slog.Info("gRPC server listening", "addr", cfg.GRPCAddr)
		errCh <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	case err := <-errCh:
		return err
	}

	slog.Info("Shutting down servers...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-shutdownCtx.Done():
		slog.Warn("graceful stop timed out")
		srv.Stop()
	}

	slog.Info("gRPC server stopped")
	return nil
}

func watchHealth(ctx context.Context, pool *pgxpool.Pool, hs *health.Server) {
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	lastStatus := checkDbStatus(ctx, pool, healthpb.HealthCheckResponse_NOT_SERVING, hs)

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastStatus = checkDbStatus(ctx, pool, lastStatus, hs)
		}
	}
}

func checkDbStatus(ctx context.Context,
	pool pinger,
	lastStatus healthpb.HealthCheckResponse_ServingStatus,
	hs *health.Server,
) healthpb.HealthCheckResponse_ServingStatus {
	pingCtx, cancel := context.WithTimeout(ctx, dbPingTimeout)
	err := pool.Ping(pingCtx)
	cancel()
	currentStatus := healthpb.HealthCheckResponse_SERVING
	if err != nil {
		currentStatus = healthpb.HealthCheckResponse_NOT_SERVING
	}
	if currentStatus != lastStatus {
		lastStatus = currentStatus
		hs.SetServingStatus("", lastStatus)
		slog.Warn("Health status changed", "current", currentStatus)
	}
	return lastStatus
}

func initGRPCServer(
	ctx context.Context,
	configParams *config.Config,
	service raidbossV1.RaidServiceServer,
) (*grpc.Server, net.Listener, error) {
	// ListenConfig, а не net.Listen: слушающий сокет тоже создается в контексте
	// (его можно отменить, пока резолвится адрес). Этого требует линтер noctx.
	var lc net.ListenConfig

	lis, err := lc.Listen(ctx, "tcp", configParams.GRPCAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	return newGRPCServer(service, configParams), lis, nil
}

func newGRPCServer(
	service raidbossV1.RaidServiceServer,
	configParams *config.Config,
) *grpc.Server {
	auth := interceptors.NewAuth(configParams.PublicServices()...)
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		// logging снаружи recovery: паника превращается в INTERNAL и попадает в лог вызова.
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryLogging,
			interceptors.UnaryRecovery,
			auth.Unary(),
			interceptors.UnaryErrorMapper,
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamLogging,
			interceptors.StreamRecovery,
			auth.Stream(),
			interceptors.StreamErrorMapper,
		),
	)

	raidbossV1.RegisterRaidServiceServer(s, service)
	if configParams.GRPCReflection {
		slog.Info("Reflection is on: ✅")
		reflection.Register(s)
	}

	return s
}
