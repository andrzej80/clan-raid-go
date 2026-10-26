package grpcapi_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/watch"
)

// Утечка горутин после тестов стрима (хендлер, hub, отправитель) валит пакет.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const waitTimeout = 10 * time.Second

// testFeeder — источник обновлений: тест вбрасывает сообщения вручную.
type testFeeder struct {
	mu    sync.Mutex
	sinks map[uuid.UUID]func(*watch.Update)
	stops int
}

func (f *testFeeder) Start(raidID uuid.UUID, sink func(*watch.Update)) (stop func(), ready <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sinks == nil {
		f.sinks = make(map[uuid.UUID]func(*watch.Update))
	}
	f.sinks[raidID] = sink
	closed := make(chan struct{})
	close(closed)
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.stops++
		delete(f.sinks, raidID)
	}, closed
}

func (f *testFeeder) emit(t testing.TB, raidID uuid.UUID, u *watch.Update) {
	t.Helper()
	f.mu.Lock()
	sink := f.sinks[raidID]
	f.mu.Unlock()
	if sink == nil {
		t.Fatalf("no source for raid %s", raidID)
	}
	sink(u)
}

func (f *testFeeder) stopped() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops
}

type stubRaids struct {
	mu    sync.Mutex
	raids map[uuid.UUID]*domain.Raid
}

func (s *stubRaids) Create(context.Context, raid.CreateParams) (*domain.Raid, error) {
	return nil, errors.New("not used")
}

func (s *stubRaids) Get(_ context.Context, id uuid.UUID) (*domain.Raid, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.raids[id]
	if !ok {
		return nil, domain.ErrRaidNotFound
	}
	return r, nil
}

type stubBoard struct{ entries []domain.LeaderboardEntry }

func (b stubBoard) Top(context.Context, uuid.UUID, int) ([]domain.LeaderboardEntry, error) {
	return b.entries, nil
}

type env struct {
	client raidbossV1.RaidServiceClient
	feeder *testFeeder
	hub    *watch.Hub
	raids  *stubRaids
}

// newEnv поднимает gRPC-сервер на bufconn с настоящим hub и ручным источником обновлений.
func newEnv(t *testing.T, cfg watch.Config, withWatch bool, dial ...grpc.DialOption) *env {
	t.Helper()
	e := &env{feeder: &testFeeder{}, raids: &stubRaids{raids: make(map[uuid.UUID]*domain.Raid)}}
	e.hub = watch.New(e.feeder, cfg)

	var opts []grpcapi.Option
	if withWatch {
		board := stubBoard{entries: []domain.LeaderboardEntry{{PlayerID: 1, TotalDamage: 100}}}
		opts = append(opts, grpcapi.WithWatch(grpcapi.NewWatchHandlers(e.raids, board, e.hub)))
	}
	svc := grpcapi.NewRaidService(opts...)

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.ChainStreamInterceptor(interceptors.StreamErrorMapper))
	raidbossV1.RegisterRaidServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, dial...)...)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	e.client = raidbossV1.NewRaidServiceClient(conn)

	// Порядок: клиент, hub (завершает стримы), сервер, listener.
	t.Cleanup(func() { _ = lis.Close() })
	t.Cleanup(srv.Stop)
	t.Cleanup(e.hub.Close)
	t.Cleanup(func() { _ = conn.Close() })
	return e
}

func (e *env) addRaid(status domain.RaidStatus, hpLeft int64) *domain.Raid {
	r := &domain.Raid{
		ID: uuid.New(), ClanID: 1, BossName: "Hydra", HPMax: 1000, HPLeft: hpLeft,
		Status: status, EndsAt: time.Now().Add(time.Hour),
	}
	e.raids.mu.Lock()
	e.raids.raids[r.ID] = r
	e.raids.mu.Unlock()
	return r
}

func hpUpdate(hp int64) *watch.Update {
	return &watch.Update{Update: &raidbossV1.WatchRaidResponse_HpLeft{HpLeft: hp}}
}

func finishedUpdate() *watch.Update {
	return &watch.Update{Update: &raidbossV1.WatchRaidResponse_Finished{
		Finished: &raidbossV1.RaidFinished{Status: raidbossV1.RaidStatus_RAID_STATUS_DEFEATED, KillerPlayerId: 7},
	}}
}

// bigLeaderboard — тяжёлое сообщение, чтобы окно flow control заполнялось за десятки сообщений.
func bigLeaderboard(entries int) *watch.Update {
	out := make([]*raidbossV1.LeaderboardEntry, entries)
	for i := range out {
		out[i] = &raidbossV1.LeaderboardEntry{PlayerId: int64(i + 1), TotalDamage: int64(1_000_000 + i)}
	}
	return &watch.Update{Update: &raidbossV1.WatchRaidResponse_Leaderboard{
		Leaderboard: &raidbossV1.GetLeaderboardResponse{Entries: out},
	}}
}

func recv(t testing.TB, stream grpc.ServerStreamingClient[raidbossV1.WatchRaidResponse]) *raidbossV1.WatchRaidResponse {
	t.Helper()
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	return msg
}

func open(t testing.TB, ctx context.Context, e *env, raidID uuid.UUID) grpc.ServerStreamingClient[raidbossV1.WatchRaidResponse] {
	t.Helper()
	stream, err := e.client.WatchRaid(ctx, &raidbossV1.WatchRaidRequest{RaidId: raidID.String()})
	if err != nil {
		t.Fatalf("WatchRaid: %v", err)
	}
	return stream
}

func timeoutCtx(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	t.Cleanup(cancel)
	return ctx
}

func TestWatchRaid_SnapshotLeaderboardUpdatesThenFinished(t *testing.T) {
	t.Parallel()
	e := newEnv(t, watch.Config{Interval: -1}, true)
	r := e.addRaid(domain.RaidActive, 900)
	stream := open(t, timeoutCtx(t), e, r.ID)

	if snap := recv(t, stream).GetSnapshot(); snap.GetId() != r.ID.String() || snap.GetHpLeft() != 900 {
		t.Fatalf("first message = %v, want snapshot of the raid", snap)
	}
	if lb := recv(t, stream).GetLeaderboard(); len(lb.GetEntries()) != 1 {
		t.Fatalf("second message = %v, want the current leaderboard", lb)
	}

	e.feeder.emit(t, r.ID, hpUpdate(500))
	if got := recv(t, stream).GetHpLeft(); got != 500 {
		t.Fatalf("hp_left = %d, want 500", got)
	}

	e.feeder.emit(t, r.ID, finishedUpdate())
	if fin := recv(t, stream).GetFinished(); fin.GetKillerPlayerId() != 7 {
		t.Fatalf("finished = %v, want killer 7", fin)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("after finished err = %v, want io.EOF (status OK)", err)
	}
}

func TestWatchRaid_AlreadyFinishedRaidGetsSnapshotAndFinished(t *testing.T) {
	t.Parallel()
	e := newEnv(t, watch.Config{Interval: -1}, true)
	r := e.addRaid(domain.RaidDefeated, 0)
	killer := int64(42)
	r.KillerPlayerID = &killer
	stream := open(t, timeoutCtx(t), e, r.ID)

	if recv(t, stream).GetSnapshot() == nil {
		t.Fatal("first message is not a snapshot")
	}
	fin := recv(t, stream).GetFinished()
	if fin.GetStatus() != raidbossV1.RaidStatus_RAID_STATUS_DEFEATED || fin.GetKillerPlayerId() != 42 {
		t.Fatalf("finished = %v, want defeated by 42", fin)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestWatchRaid_Errors(t *testing.T) {
	t.Parallel()
	e := newEnv(t, watch.Config{Interval: -1}, true)

	tests := []struct {
		name   string
		raidID string
		want   codes.Code
	}{
		{"unknown raid", uuid.NewString(), codes.NotFound},
		{"bad uuid", "nope", codes.InvalidArgument},
	}
	for _, tc := range tests {
		stream, err := e.client.WatchRaid(timeoutCtx(t), &raidbossV1.WatchRaidRequest{RaidId: tc.raidID})
		if err == nil {
			_, err = stream.Recv()
		}
		if got := status.Code(err); got != tc.want {
			t.Errorf("%s: code = %v, want %v (err: %v)", tc.name, got, tc.want, err)
		}
	}
}

func TestWatchRaid_NotEnabledIsUnimplemented(t *testing.T) {
	t.Parallel()
	e := newEnv(t, watch.Config{}, false)

	stream, err := e.client.WatchRaid(timeoutCtx(t), &raidbossV1.WatchRaidRequest{RaidId: uuid.NewString()})
	if err == nil {
		_, err = stream.Recv()
	}
	if got := status.Code(err); got != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented", got)
	}
}

func TestWatchRaid_ClientCancelReleasesSubscription(t *testing.T) {
	t.Parallel()
	e := newEnv(t, watch.Config{Interval: -1}, true)
	r := e.addRaid(domain.RaidActive, 900)
	ctx, cancel := context.WithCancel(t.Context())
	stream := open(t, ctx, e, r.ID)
	recv(t, stream) // snapshot

	cancel()

	deadline := time.After(waitTimeout)
	for e.feeder.stopped() == 0 {
		select {
		case <-deadline:
			t.Fatal("source was not stopped after the client left")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Клиент открыл стрим и не читает его. Hub не должен ни ждать его, ни задерживать остальных,
// а сам медленный клиент в итоге получает RESOURCE_EXHAUSTED.
func TestWatchRaid_SlowClientIsDisconnectedWithoutDelayingOthers(t *testing.T) {
	t.Parallel()
	// Окно HTTP/2 фиксируем на минимуме: динамическое окно (BDP) растёт и сильно оттягивает блокировку Send.
	e := newEnv(t, watch.Config{BufferSize: 4, Interval: -1}, true,
		grpc.WithInitialWindowSize(65535), grpc.WithInitialConnWindowSize(65535))
	r := e.addRaid(domain.RaidActive, 900)

	slow := open(t, timeoutCtx(t), e, r.ID)
	recv(t, slow) // snapshot
	recv(t, slow) // leaderboard; дальше медленный клиент не читает

	fast := open(t, timeoutCtx(t), e, r.ID)
	recv(t, fast)
	recv(t, fast)

	const updates = 400
	var worst time.Duration
	for range updates {
		start := time.Now()
		e.feeder.emit(t, r.ID, bigLeaderboard(200))
		recv(t, fast)
		worst = max(worst, time.Since(start))
	}
	t.Logf("worst delivery latency to the fast client: %v", worst)
	if worst > time.Second {
		t.Fatalf("fast client was delayed by the slow one: %v", worst)
	}

	// Теперь медленный клиент наконец читает: сначала то, что успело уйти, затем статус.
	var err error
	for err == nil {
		_, err = slow.Recv()
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("slow client code = %v, want ResourceExhausted (err: %v)", got, err)
	}
}

// 1000+ одновременных стримов на одном экземпляре: каждый получает snapshot и обновление,
// после отключения клиентов источник останавливается, а goleak в TestMain проверяет, что
// горутины стримов не утекли.
func TestWatchRaid_ThousandStreams(t *testing.T) {
	t.Parallel()
	const streams = 1200
	e := newEnv(t, watch.Config{Interval: -1}, true)
	r := e.addRaid(domain.RaidActive, 900)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var opened, got sync.WaitGroup
	opened.Add(streams)
	got.Add(streams)
	errs := make(chan error, streams)
	for range streams {
		go func() {
			stream, err := e.client.WatchRaid(ctx, &raidbossV1.WatchRaidRequest{RaidId: r.ID.String()})
			if err != nil {
				errs <- err
				opened.Done()
				got.Done()
				return
			}
			// snapshot и leaderboard приходят до того, как стрим считается подписанным.
			for range 2 {
				if _, err := stream.Recv(); err != nil {
					errs <- err
					opened.Done()
					got.Done()
					return
				}
			}
			opened.Done()
			msg, err := stream.Recv()
			if err != nil || msg.GetHpLeft() != 500 {
				errs <- errors.Join(err, errors.New("no hp_left=500 update"))
			}
			got.Done()
		}()
	}

	waitOrFail(t, &opened, "streams to open")
	e.feeder.emit(t, r.ID, hpUpdate(500))
	waitOrFail(t, &got, "streams to receive the update")

	close(errs)
	for err := range errs {
		t.Errorf("stream: %v", err)
	}

	cancel() // клиенты уходят: подписки закрываются, источник останавливается
	deadline := time.After(waitTimeout)
	for e.feeder.stopped() == 0 {
		select {
		case <-deadline:
			t.Fatal("source was not stopped after all clients left")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitOrFail(t testing.TB, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
}
