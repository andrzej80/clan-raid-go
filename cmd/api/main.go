package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/storage/postgres"
	"github.com/andrzej80/clan-raid-go/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	shutdownTimeout        = 10 * time.Second
	healthCheckInterval    = 2 * time.Second
	dbPingTimeout          = 500 * time.Millisecond
	defaultMaxConns        = 5
	defaultMinConns        = 0
	defaultMaxConnIdleTime = 5 * time.Minute
	defaultMaxConnLifetime = 30 * time.Minute
)

type Config struct {
	gRPCAddr       string
	gRPCReflection bool
}

type raidService struct {
	raidbossV1.UnimplementedRaidServiceServer
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}

	slog.Info("gRPC server stopped", "port", cfg.gRPCAddr, "isDevMode", cfg.gRPCReflection)
}

func run(cfg *Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx)
	if err != nil {
		return err
	}
	defer shutdownTelemetry(ctx)

	dbCfg, err := loadDbConfig()
	if err != nil {
		return err
	}
	dbPool, err := postgres.NewPool(ctx, dbCfg)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	service := newRaidService()

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
		slog.Info("gRPC server listening", "addr", cfg.gRPCAddr)
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
	tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdownTelemetry(tctx)
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
	pool *pgxpool.Pool,
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

func newRaidService() *raidService {
	return &raidService{}
}

func loadConfig() (*Config, error) {
	host := os.Getenv("APP_HOST")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "9090"
	}
	numericPort, err := strconv.Atoi(appPort)
	if err != nil {
		return nil, fmt.Errorf("invalid APP_PORT %q: %w", appPort, err)
	}
	if numericPort > 65535 || numericPort < 1 {
		return nil, fmt.Errorf("APP_PORT %d out of range 1..65535", numericPort)
	}
	cfg := &Config{
		gRPCAddr:       net.JoinHostPort(host, appPort),
		gRPCReflection: false,
	}

	isDevMode := os.Getenv("GRPC_REFLECTION")
	if isDevMode == "" {
		cfg.gRPCReflection = false
	} else {
		parsed, err := strconv.ParseBool(isDevMode)
		if err != nil {
			return nil, fmt.Errorf("invalid GRPC_REFLECTION %q: %w", isDevMode, err)
		}
		cfg.gRPCReflection = parsed
	}
	return cfg, nil
}

func loadDbConfig() (*postgres.DbConfig, error) {
	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		return nil, fmt.Errorf("invalid DATABASE_URL")
	}

	cfg := &postgres.DbConfig{
		Dsn:             dbUrl,
		MaxConns:        getEnvInt("PG_MAX_CONNS", defaultMaxConns),
		MinConns:        getEnvInt("PG_MIN_CONNS", defaultMinConns),
		MaxConnIdleTime: getEnvDuration("PG_MAX_CONN_IDLE_TIME", defaultMaxConnIdleTime),
		MaxConnLifetime: getEnvDuration("PG_MAX_CONN_LIFETIME", defaultMaxConnLifetime),
	}
	return cfg, nil
}

func getEnvInt(key string, defaultValue int32) int32 {
	fromEnv := os.Getenv(key)
	if fromEnv == "" {
		slog.Warn("Value is set to default", "valueName", key)
		return defaultValue
	}
	parsedValue, err := strconv.Atoi(fromEnv)
	if err != nil {
		slog.Warn("Value is set to default", "valueName", key, "rawData", fromEnv)
		return defaultValue
	}
	return int32(parsedValue)
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	fromEnv := os.Getenv(key)
	if fromEnv == "" {
		slog.Warn("Value is set to default", "valueName", key)
		return defaultValue
	}
	parsedValue, err := time.ParseDuration(fromEnv)
	if err != nil {
		slog.Warn("Value is set to default", "valueName", key, "rawData", fromEnv)
		return defaultValue
	}
	return parsedValue
}

func initGRPCServer(
	ctx context.Context,
	configParams *Config,
	service raidbossV1.RaidServiceServer,
) (*grpc.Server, net.Listener, error) {
	// ListenConfig, а не net.Listen: слушающий сокет тоже создается в контексте
	// (его можно отменить, пока резолвится адрес). Этого требует линтер noctx.
	var lc net.ListenConfig

	lis, err := lc.Listen(ctx, "tcp", configParams.gRPCAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	return newGRPCServer(service, configParams), lis, nil
}

func newGRPCServer(
	service raidbossV1.RaidServiceServer,
	configParams *Config,
) *grpc.Server {
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	raidbossV1.RegisterRaidServiceServer(s, service)
	if configParams.gRPCReflection {
		slog.Info("Reflection is on: ✅")
		reflection.Register(s)
	}

	return s
}
