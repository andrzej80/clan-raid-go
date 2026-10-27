package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/app"
	"github.com/andrzej80/clan-raid-go/internal/breaker"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/ratelimit"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
	"github.com/andrzej80/clan-raid-go/internal/watch"
)

const (
	shutdownTimeout     = 10 * time.Second
	healthCheckInterval = 2 * time.Second
	dbPingTimeout       = 500 * time.Millisecond
	kafkaPingTimeout    = 2 * time.Second

	// Правило 4 из README: 5 ударов в секунду на игрока, burst 10.
	attackRatePerSecond = 5
	attackBurst         = 10
)

type pinger interface {
	Ping(ctx context.Context) error
}

func main() {
	app.Main("api", run)
}

func run(ctx context.Context, cfg *config.Config) error {
	flushTelemetry, err := app.SetupTelemetry(ctx)
	if err != nil {
		return err
	}
	defer flushTelemetry()

	dbPool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	if cfg.Redis.URL == "" {
		return errors.New("REDIS_URL is required")
	}
	redisClient, err := rdb.New(ctx, &cfg.Redis)
	if err != nil {
		return err
	}
	defer func() { _ = redisClient.Close() }() //nolint:gosec // закрытие при выходе, ошибку обработать нечем

	hub, closeHub := newWatchHub(redisClient.Redis(), cfg.Watch)
	defer closeHub()

	producer, err := kafka.NewProducer(kafka.Config{Brokers: cfg.Kafka.Brokers, Topic: cfg.Kafka.HitsTopic})
	if err != nil {
		return err
	}
	// Продюсер закрывается после GracefulStop (см. shutdown): идущие Attack ждут подтверждения брокера.
	// Defer — страховка для раннего выхода по ошибке; OnceFunc не даст закрыть дважды.
	closeKafka := sync.OnceFunc(func() { closeProducer(producer) }) //nolint:contextcheck // закрытие после отмены ctx сигналом
	defer closeKafka()

	raidRepo := pg.NewRaidRepo(dbPool)
	attacks, err := newAttackService(ctx, raidRepo, pg.NewCatalogRepo(dbPool), producer)
	if err != nil {
		return err
	}
	leaderboard, err := raid.NewLeaderboardService(raidRepo, rdb.NewBoard(redisClient.Redis()), pg.NewDamageRepo(dbPool))
	if err != nil {
		return err
	}
	raids := raid.NewService(raidRepo)
	service := grpcapi.NewRaidService(
		grpcapi.WithRaids(grpcapi.NewRaidHandlers(raids)),
		grpcapi.WithAttacks(grpcapi.NewAttackHandlers(attacks)),
		grpcapi.WithLeaderboard(grpcapi.NewLeaderboardHandlers(leaderboard)),
		grpcapi.WithRewards(grpcapi.NewRewardsHandlers(
			raid.NewRewardsService(pg.NewCatalogRepo(dbPool), pg.NewRewardsRepo(dbPool)),
		)),
		grpcapi.WithWatch(grpcapi.NewWatchHandlers(raids, leaderboard, hub)),
	)
	// Лимит общий для всех экземпляров api (Redis). При сбое Redis лимит ослабляется до
	// «на экземпляр» (Memory), но не пропадает; fail open остаётся только на случай ошибки обоих.
	limiter := ratelimit.NewFallback(
		ratelimit.NewRedis(redisClient.Redis(), attackRatePerSecond, attackBurst),
		ratelimit.NewMemory(attackRatePerSecond, attackBurst),
	)

	srv, lis, err := initGRPCServer(ctx, cfg, service, limiter)
	if err != nil {
		return err
	}

	hs := health.NewServer()
	defer hs.Shutdown()
	healthpb.RegisterHealthServer(srv, hs)

	// Redis здесь намеренно нет: это не критичная зависимость. Лимит при его сбое держит Memory,
	// а Attack от Redis не зависит. Если появится чтение, которому некуда деться без Redis
	// (например, GetLeaderboard без фоллбэка на Postgres), решение стоит пересмотреть.
	go watchHealth(ctx, dependencies{
		{name: "postgres", pinger: dbPool, timeout: dbPingTimeout},
		{name: "kafka", pinger: producer, timeout: kafkaPingTimeout},
	}, hs)

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
	// Стримы WatchRaid бесконечны: GracefulStop ждал бы их до таймаута. Остановка hub завершает
	// их статусом UNAVAILABLE, клиент переподключится к другому экземпляру.
	hub.Close()
	shutdown(srv, closeKafka)
	return nil
}

// newWatchHub создаёт hub стрима WatchRaid поверх Redis pub/sub. closeHub останавливает hub, а
// затем источник: hub гасит подписки на рейды, и только после этого можно дождаться их горутин.
func newWatchHub(rc redis.UniversalClient, cfg config.WatchConfig) (hub *watch.Hub, closeHub func()) {
	updates := rdb.NewUpdates(rc)
	hub = watch.New(updates, watch.Config{BufferSize: cfg.BufferSize, Interval: cfg.UpdateInterval})
	return hub, func() {
		hub.Close()
		updates.Close()
	}
}

// shutdown сначала останавливает gRPC-сервер, и только потом закрывает продюсера: идущие Attack
// ждут подтверждения брокера, и закрытие продюсера раньше оборвало бы уже принятые запросы.
func shutdown(srv *grpc.Server, closeProducer func()) {
	stopServer(srv)
	slog.Info("gRPC server stopped")

	// Все RPC завершены (или принудительно остановлены), теперь можно сбросить буфер продюсера.
	closeProducer()
}

// stopServer ждёт завершения идущих RPC; по таймауту останавливает сервер принудительно.
func stopServer(srv *grpc.Server) {
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
}

func closeProducer(p *kafka.Producer) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		slog.Warn("kafka producer flush on shutdown", "err", err)
	}
}

// dependency — внешняя зависимость сервиса со своим таймаутом проверки: у Postgres и Kafka
// разное время отклика, и один общий таймаут либо ложно роняет health, либо слишком долго ждёт БД.
type dependency struct {
	name    string
	pinger  pinger
	timeout time.Duration
}

// dependencies считает сервис здоровым, только если отвечают все зависимости.
type dependencies []dependency

func (d dependencies) Ping(ctx context.Context) error {
	for _, dep := range d {
		pingCtx, cancel := context.WithTimeout(ctx, dep.timeout)
		err := dep.pinger.Ping(pingCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("%s: %w", dep.name, err)
		}
	}
	return nil
}

func watchHealth(ctx context.Context, deps pinger, hs *health.Server) {
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	lastStatus := checkHealth(ctx, deps, healthpb.HealthCheckResponse_NOT_SERVING, hs)

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastStatus = checkHealth(ctx, deps, lastStatus, hs)
		}
	}
}

func checkHealth(ctx context.Context,
	deps pinger,
	lastStatus healthpb.HealthCheckResponse_ServingStatus,
	hs *health.Server,
) healthpb.HealthCheckResponse_ServingStatus {
	err := deps.Ping(ctx)
	currentStatus := healthpb.HealthCheckResponse_SERVING
	if err != nil {
		currentStatus = healthpb.HealthCheckResponse_NOT_SERVING
	}
	if currentStatus != lastStatus {
		lastStatus = currentStatus
		hs.SetServingStatus("", lastStatus)
		slog.Warn("Health status changed", "current", currentStatus, "err", err)
	}
	return lastStatus
}

func initGRPCServer(
	ctx context.Context,
	configParams *config.Config,
	service raidbossV1.RaidServiceServer,
	limiter interceptors.Limiter,
) (*grpc.Server, net.Listener, error) {
	// ListenConfig, а не net.Listen: слушающий сокет тоже создается в контексте
	// (его можно отменить, пока резолвится адрес). Этого требует линтер noctx.
	var lc net.ListenConfig

	lis, err := lc.Listen(ctx, "tcp", configParams.GRPCAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	return newGRPCServer(service, configParams, limiter), lis, nil
}

func newGRPCServer(
	service raidbossV1.RaidServiceServer,
	configParams *config.Config,
	limiter interceptors.Limiter,
) *grpc.Server {
	auth := interceptors.NewAuth(configParams.PublicServices()...)
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		// logging снаружи recovery: паника превращается в INTERNAL и попадает в лог вызова.
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryLogging,
			interceptors.UnaryRecovery,
			auth.Unary(),
			interceptors.UnaryRateLimit(limiter, raidbossV1.RaidService_Attack_FullMethodName),
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

// newAttackService собирает приём ударов: продюсер Kafka под предохранителем. Пока очередь недоступна,
// предохранитель отвечает UNAVAILABLE сразу, а не после таймаута доставки. Фоновая проверка очереди
// работает до отмены ctx.
func newAttackService(
	ctx context.Context, raids domain.RaidRepository, players domain.PlayerReader, producer *kafka.Producer,
) (*raid.AttackService, error) {
	hitQueue, err := raid.NewGuardedPublisher(ctx, producer, producer, breaker.Config{})
	if err != nil {
		return nil, err
	}
	go hitQueue.Run(ctx)
	return raid.NewAttackService(raids, players, hitQueue), nil
}
