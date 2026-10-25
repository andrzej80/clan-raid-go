package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	"github.com/andrzej80/clan-raid-go/internal/kafka"
	"github.com/andrzej80/clan-raid-go/internal/raid"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func startTestServer(t *testing.T, cfg *config.Config) *grpc.ClientConn {
	t.Helper()

	return startTestServerWith(t, cfg, grpcapi.NewRaidService(nil, nil, nil, nil))
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

	conn, _ := startServer(t, cfg, svc, limiter)
	return conn
}

// startServer возвращает и клиента, и сам сервер: тесту остановки нужно вызвать GracefulStop.
func startServer(
	t *testing.T,
	cfg *config.Config,
	svc raidbossV1.RaidServiceServer,
	limiter interceptors.Limiter,
) (*grpc.ClientConn, *grpc.Server) {
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

	return conn, srv
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

func TestCheckHealth(t *testing.T) {
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
			gotStatus := checkHealth(
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

// failureCountingPublisher считает ошибки продюсера: при правильном порядке остановки
// ни один идущий Attack не должен упереться в уже закрытого продюсера.
type failureCountingPublisher struct {
	inner    *kafka.Producer
	failures atomic.Int32
}

func (p *failureCountingPublisher) Publish(ctx context.Context, hit domain.Hit) error {
	err := p.inner.Publish(ctx, hit)
	if err != nil {
		p.failures.Add(1)
	}
	return err
}

// consumeHitIDs читает топик с начала и возвращает hit_id всех записей.
func consumeHitIDs(t *testing.T, brokers []string) map[string]bool {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("raid.hits"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer client.Close()

	ids := map[string]bool{}
	// Пустая выборка после нескольких опросов подряд значит, что записи закончились.
	for empty := 0; empty < 3; {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		fetches := client.PollFetches(ctx)
		cancel()

		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			var event eventsV1.HitEvent
			if err := proto.Unmarshal(r.Value, &event); err != nil {
				t.Errorf("unmarshal: %v", err)
				return
			}
			ids[event.GetHitId()] = true
			got++
		})
		if got == 0 {
			empty++
		} else {
			empty = 0
		}
	}
	return ids
}

// Критерий этапа 2: при shutdown ни один удар с успешным ответом не теряется.
// Под нагрузкой останавливаем сервер так же, как это делает run: сначала GracefulStop, потом закрытие продюсера.
func TestShutdown_NoAcceptedHitIsLost(t *testing.T) {
	t.Parallel()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(12, "raid.hits"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	brokers := cluster.ListenAddrs()

	producer, err := kafka.NewProducer(kafka.Config{Brokers: brokers, Topic: "raid.hits"})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	publisher := &failureCountingPublisher{inner: producer}

	raids := newFakeRaids(attackerClan)
	raidID := uuid.New()
	raids.raids[raidID] = domain.Raid{
		ID: raidID, ClanID: attackerClan, BossName: "Dragon", HPMax: 1_000_000, HPLeft: 1_000_000,
		Status: domain.RaidActive, StartedAt: time.Now().Add(-time.Hour), EndsAt: time.Now().Add(time.Hour),
	}
	players := fakePlayers{attackerID: {ID: attackerID, ClanID: attackerClan, Name: "p", Power: attackerPower}}
	svc := grpcapi.NewRaidService(raid.NewService(raids), raid.NewAttackService(raids, players, publisher), nil, nil)
	conn, srv := startServer(t, &config.Config{}, svc, allowAll{})
	client := raidbossV1.NewRaidServiceClient(conn)

	const (
		workers      = 50
		minAccepted  = 200
		shutdownWait = 15 * time.Second
	)
	var (
		mu       sync.Mutex
		accepted []string
		wg       sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			for {
				key := uuid.NewString()
				ctx := playerCtx(t, attackerID)
				resp, err := client.Attack(ctx, &raidbossV1.AttackRequest{RaidId: raidID.String(), IdempotencyKey: key})
				if err != nil {
					return // сервер остановился: новые вызовы больше не принимаются
				}
				mu.Lock()
				accepted = append(accepted, resp.GetHitId())
				mu.Unlock()
			}
		})
	}

	// Ждём, пока наберётся нагрузка, и останавливаем сервер посреди потока запросов.
	for {
		mu.Lock()
		n := len(accepted)
		mu.Unlock()
		if n >= minAccepted {
			break
		}
		<-time.After(time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		shutdown(srv, func() {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			_ = producer.Close(ctx)
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownWait):
		t.Fatal("shutdown did not finish")
	}
	wg.Wait()

	if n := publisher.failures.Load(); n != 0 {
		t.Fatalf("%d in-flight Attack calls failed during shutdown: the producer was closed too early", n)
	}

	stored := consumeHitIDs(t, brokers)
	mu.Lock()
	defer mu.Unlock()
	if len(accepted) < minAccepted {
		t.Fatalf("only %d hits accepted", len(accepted))
	}
	for _, id := range accepted {
		if !stored[id] {
			t.Fatalf("hit %s was acknowledged to the client but is missing in Kafka", id)
		}
	}
	t.Logf("accepted %d hits, stored %d records", len(accepted), len(stored))
}

// slowPinger отвечает успехом через delay, но прерывается, если контекст отменён раньше.
type slowPinger struct{ delay time.Duration }

func (p slowPinger) Ping(ctx context.Context) error {
	select {
	case <-time.After(p.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestDependencies_PerDependencyTimeout(t *testing.T) {
	t.Parallel()

	slow := slowPinger{delay: 100 * time.Millisecond}

	// Один и тот же медленный ответ: терпимый для Kafka и недопустимый для БД.
	patient := dependencies{{name: "kafka", pinger: slow, timeout: time.Second}}
	if err := patient.Ping(context.Background()); err != nil {
		t.Fatalf("slow dependency with a generous timeout must be healthy: %v", err)
	}

	strict := dependencies{{name: "postgres", pinger: slow, timeout: 10 * time.Millisecond}}
	err := strict.Ping(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

func TestDependencies_ErrorNamesFailedDependency(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection refused")
	deps := dependencies{
		{name: "postgres", pinger: fakePinger{}, timeout: time.Second},
		{name: "kafka", pinger: fakePinger{err: boom}, timeout: time.Second},
	}

	err := deps.Ping(context.Background())
	if !errors.Is(err, boom) || err.Error() != "kafka: connection refused" {
		t.Fatalf("err = %v, want it to name the failed dependency and wrap the cause", err)
	}
}

func TestDependencies_AllHealthy(t *testing.T) {
	t.Parallel()

	deps := dependencies{
		{name: "postgres", pinger: fakePinger{}, timeout: time.Second},
		{name: "kafka", pinger: fakePinger{}, timeout: time.Second},
	}
	if err := deps.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
