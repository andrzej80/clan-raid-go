package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// fakeWatch — сервер, у которого работает только WatchRaid; handler получает номер вызова с нуля.
type fakeWatch struct {
	raidbossV1.UnimplementedRaidServiceServer
	calls   atomic.Int32
	handler func(call int, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error
}

func (f *fakeWatch) WatchRaid(_ *raidbossV1.WatchRaidRequest, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error {
	return f.handler(int(f.calls.Add(1))-1, stream)
}

func newWatchClient(t *testing.T, srv *fakeWatch) raidbossV1.RaidServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	raidbossV1.RegisterRaidServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raidbossV1.NewRaidServiceClient(conn)
}

func sendAll(stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error {
	for _, u := range []*raidbossV1.WatchRaidResponse{
		{Update: &raidbossV1.WatchRaidResponse_Snapshot{Snapshot: &raidbossV1.Raid{}}},
		{Update: &raidbossV1.WatchRaidResponse_Leaderboard{Leaderboard: &raidbossV1.GetLeaderboardResponse{}}},
		{Update: &raidbossV1.WatchRaidResponse_HpLeft{HpLeft: 10}},
		{Update: &raidbossV1.WatchRaidResponse_Finished{Finished: &raidbossV1.RaidFinished{}}},
	} {
		if err := stream.Send(u); err != nil {
			return err
		}
	}
	return nil
}

func runWatch(t *testing.T, client raidbossV1.RaidServiceClient) *watchReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ws := newWatchStats()
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchLoop(ctx, client, &target{raidID: "raid"}, ws)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("watchLoop did not finish")
	}
	return ws.report()
}

func TestWatchLoopCountsMessagesAndStopsOnFinish(t *testing.T) {
	t.Parallel()

	srv := &fakeWatch{handler: func(_ int, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error {
		return sendAll(stream)
	}}
	r := runWatch(t, newWatchClient(t, srv))

	if r.Snapshots != 1 || r.Leaderboards != 1 || r.HPUpdates != 1 || r.Finished != 1 {
		t.Fatalf("messages = %+v", r)
	}
	if r.Connects != 1 || r.Reconnects != 0 || srv.calls.Load() != 1 {
		t.Fatalf("connects=%d reconnects=%d calls=%d, want a single connection", r.Connects, r.Reconnects, srv.calls.Load())
	}
}

func TestWatchLoopReconnectsAfterSlowKick(t *testing.T) {
	t.Parallel()

	srv := &fakeWatch{handler: func(call int, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error {
		if call == 0 {
			_ = stream.Send(&raidbossV1.WatchRaidResponse{
				Update: &raidbossV1.WatchRaidResponse_Snapshot{Snapshot: &raidbossV1.Raid{}},
			})
			return status.Error(codes.ResourceExhausted, "slow client")
		}
		return sendAll(stream)
	}}
	r := runWatch(t, newWatchClient(t, srv))

	if r.SlowKicked != 1 || r.Reconnects != 1 || r.Connects != 2 || r.Finished != 1 {
		t.Fatalf("report = %+v, want 1 slow kick, 1 reconnect, 2 connects, finished", r)
	}
	if r.Snapshots != 2 {
		t.Fatalf("snapshots = %d, want one per connection", r.Snapshots)
	}
}

func TestWatchLoopDoesNotRetryNotFound(t *testing.T) {
	t.Parallel()

	srv := &fakeWatch{handler: func(int, grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error {
		return status.Error(codes.NotFound, "no raid")
	}}
	r := runWatch(t, newWatchClient(t, srv))

	if r.Failures != 1 || r.Reconnects != 0 || srv.calls.Load() != 1 {
		t.Fatalf("report = %+v calls=%d, want no retry", r, srv.calls.Load())
	}
}

func TestWatchLoopStopsOnCancel(t *testing.T) {
	t.Parallel()

	sent := make(chan struct{})
	srv := &fakeWatch{handler: func(_ int, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse]) error {
		_ = stream.Send(&raidbossV1.WatchRaidResponse{
			Update: &raidbossV1.WatchRaidResponse_Snapshot{Snapshot: &raidbossV1.Raid{}},
		})
		close(sent)
		<-stream.Context().Done()
		return stream.Context().Err()
	}}
	client := newWatchClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	ws := newWatchStats()
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchLoop(ctx, client, &target{raidID: "raid"}, ws)
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("server never sent the snapshot")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not stop after cancel")
	}
}
