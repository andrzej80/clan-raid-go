package raidwatch

import (
	"context"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// drain забирает из закрытой очереди всё, что в ней осталось.
func drain(b *mailbox) []tea.Msg {
	b.close()
	var out []tea.Msg
	for {
		msg, ok := b.next()
		if !ok {
			return out
		}
		out = append(out, msg)
	}
}

func TestMailbox_CoalescesAndDropsStaleOnSnapshot(t *testing.T) {
	t.Parallel()
	b := newMailbox()
	b.put(HPMsg{HPLeft: 90})
	b.put(HitsMsg{HitsTotal: 1})
	b.put(HPMsg{HPLeft: 80}) // заменяет предыдущий HP
	b.put(ConnMsg{State: ConnReconnecting})
	b.put(SnapshotMsg{HPLeft: 70}) // новый стрим: устаревшие HP и hits из очереди выбрасываются
	b.put(HPMsg{HPLeft: 60})
	b.put(HPMsg{HPLeft: 50})
	b.put(FinishedMsg{})

	want := []tea.Msg{
		ConnMsg{State: ConnReconnecting}, SnapshotMsg{HPLeft: 70}, HPMsg{HPLeft: 50}, FinishedMsg{},
	}
	if got := drain(b); !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %#v, want %#v", got, want)
	}
}

func TestMailbox_PutNeverBlocks(t *testing.T) {
	t.Parallel()
	b := newMailbox()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100_000 { // никто не читает
			b.put(HPMsg{HPLeft: int64(i)})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("put blocked without a reader")
	}
	if got := drain(b); len(got) != 1 || got[0] != (HPMsg{HPLeft: 99_999}) {
		t.Fatalf("queue = %#v, want only the last HP", got)
	}
}

func TestJitterStaysInRange(t *testing.T) {
	t.Parallel()
	d := 400 * time.Millisecond
	if got := jitter(d, func() float64 { return 0 }); got != d/2 {
		t.Fatalf("min jitter = %v, want %v", got, d/2)
	}
	if got := jitter(d, func() float64 { return 0.999999 }); got < d*99/100 || got > d {
		t.Fatalf("max jitter = %v, want close to %v", got, d)
	}
}

// fakeStream отдаёт заранее заданные сообщения, затем ошибку.
type fakeStream struct {
	grpc.ClientStream
	msgs    []*raidbossV1.WatchRaidResponse
	end     error
	drained chan struct{} // закрывается, когда отдано последнее сообщение; необязателен
}

func (s *fakeStream) Recv() (*raidbossV1.WatchRaidResponse, error) {
	if len(s.msgs) == 0 {
		return nil, s.end
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	if len(s.msgs) == 0 && s.drained != nil {
		close(s.drained)
	}
	return m, nil
}

// fakeClient на каждый вызов WatchRaid отдаёт следующую попытку; после последней блокируется до ctx.Done().
type fakeClient struct {
	mu       sync.Mutex
	attempts []fakeAttempt
	calls    int
	players  []string
}

type fakeAttempt struct {
	openErr error
	msgs    []*raidbossV1.WatchRaidResponse
	end     error
	drained chan struct{}
}

func (c *fakeClient) WatchRaid(
	ctx context.Context, _ *raidbossV1.WatchRaidRequest, _ ...grpc.CallOption,
) (grpc.ServerStreamingClient[raidbossV1.WatchRaidResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	md, _ := metadata.FromOutgoingContext(ctx)
	c.players = append(c.players, md.Get(playerIDHeader)...)
	i := c.calls
	c.calls++
	if i >= len(c.attempts) {
		return &fakeStream{end: context.Canceled}, nil
	}
	a := c.attempts[i]
	if a.openErr != nil {
		return nil, a.openErr
	}
	return &fakeStream{msgs: a.msgs, end: a.end, drained: a.drained}, nil
}

func snapshot(hp int64) *raidbossV1.WatchRaidResponse {
	return &raidbossV1.WatchRaidResponse{Update: &raidbossV1.WatchRaidResponse_Snapshot{
		Snapshot: &raidbossV1.Raid{HpMax: 100, HpLeft: hp, BossName: "boss"},
	}}
}

func hpUpdate(hp int64) *raidbossV1.WatchRaidResponse {
	return &raidbossV1.WatchRaidResponse{Update: &raidbossV1.WatchRaidResponse_HpLeft{HpLeft: hp}}
}

func finished() *raidbossV1.WatchRaidResponse {
	return &raidbossV1.WatchRaidResponse{Update: &raidbossV1.WatchRaidResponse_Finished{
		Finished: &raidbossV1.RaidFinished{Status: raidbossV1.RaidStatus_RAID_STATUS_DEFEATED, KillerPlayerId: 7},
	}}
}

func fastConfig() WatchConfig {
	return WatchConfig{
		RaidID: "r", PlayerID: 5,
		BackoffMin: time.Millisecond, BackoffMax: 4 * time.Millisecond,
		Rand: func() float64 { return 0.5 },
	}
}

// collect запускает Watch и возвращает все сообщения, пришедшие в send, после его завершения.
func collect(ctx context.Context, c watchClient, cfg WatchConfig) []tea.Msg {
	var (
		mu  sync.Mutex
		got []tea.Msg
	)
	Watch(ctx, c, cfg, func(m tea.Msg) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	})
	return got
}

func TestWatch_ReconnectsAndGetsNewSnapshot(t *testing.T) {
	t.Parallel()
	c := &fakeClient{attempts: []fakeAttempt{
		{msgs: []*raidbossV1.WatchRaidResponse{snapshot(90)}, end: status.Error(codes.Unavailable, "restart")},
		{openErr: status.Error(codes.Unavailable, "still down")},
		{msgs: []*raidbossV1.WatchRaidResponse{snapshot(40), hpUpdate(30), finished()}},
	}}

	got := collect(t.Context(), c, fastConfig())

	want := []tea.Msg{
		SnapshotMsg{BossName: "boss", HPMax: 100, HPLeft: 90},
		ConnMsg{State: ConnReconnecting},
		ConnMsg{State: ConnReconnecting},
		SnapshotMsg{BossName: "boss", HPMax: 100, HPLeft: 40},
		HPMsg{HPLeft: 30},
		FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_DEFEATED, KillerPlayerID: 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("messages = %#v\nwant       %#v", got, want)
	}
	if c.calls != 3 {
		t.Fatalf("WatchRaid calls = %d, want 3", c.calls)
	}
	for _, p := range c.players {
		if p != "5" {
			t.Fatalf("x-player-id = %q, want 5", p)
		}
	}
}

func TestWatch_ServerClosedWithoutFinishedReconnects(t *testing.T) {
	t.Parallel()
	c := &fakeClient{attempts: []fakeAttempt{
		{msgs: []*raidbossV1.WatchRaidResponse{snapshot(90)}, end: io.EOF},
		{msgs: []*raidbossV1.WatchRaidResponse{snapshot(80), finished()}},
	}}
	got := collect(t.Context(), c, fastConfig())
	if c.calls != 2 {
		t.Fatalf("WatchRaid calls = %d, want 2; messages %#v", c.calls, got)
	}
}

func TestWatch_FatalErrorStopsWithoutRetry(t *testing.T) {
	t.Parallel()
	notFound := status.Error(codes.NotFound, "no such raid")
	c := &fakeClient{attempts: []fakeAttempt{{openErr: notFound}, {openErr: notFound}}}

	got := collect(t.Context(), c, fastConfig())

	if c.calls != 1 {
		t.Fatalf("WatchRaid calls = %d, want 1 (no retry on NotFound)", c.calls)
	}
	if len(got) != 1 {
		t.Fatalf("messages = %#v, want one ErrorMsg", got)
	}
	if e, ok := got[0].(ErrorMsg); !ok || status.Code(e.Err) != codes.NotFound {
		t.Fatalf("message = %#v, want ErrorMsg(NotFound)", got[0])
	}
}

func TestWatch_CancelDuringBackoffReturns(t *testing.T) {
	t.Parallel()
	c := &fakeClient{attempts: []fakeAttempt{{openErr: status.Error(codes.Unavailable, "down")}}}
	cfg := fastConfig()
	cfg.BackoffMin, cfg.BackoffMax = time.Hour, time.Hour // без отмены ждали бы вечно

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		collect(ctx, c, cfg)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Watch did not return after cancel")
	}
}

func TestWatch_SlowReceiverDoesNotBlockReader(t *testing.T) {
	t.Parallel()
	msgs := []*raidbossV1.WatchRaidResponse{snapshot(100000)}
	for hp := int64(99999); hp > 0; hp-- {
		msgs = append(msgs, hpUpdate(hp))
	}
	msgs = append(msgs, finished())
	drained := make(chan struct{})
	c := &fakeClient{attempts: []fakeAttempt{{msgs: msgs, drained: drained}}}

	release := make(chan struct{})
	var (
		mu  sync.Mutex
		got []tea.Msg
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Watch(t.Context(), c, fastConfig(), func(m tea.Msg) {
			<-release // «UI» стоит, пока читатель не прочитал весь стрим
			mu.Lock()
			got = append(got, m)
			mu.Unlock()
		})
	}()

	// Читатель обязан дойти до конца стрима, хотя send ещё ничего не принял.
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("reader is blocked by a stalled receiver")
	}
	time.Sleep(50 * time.Millisecond) // последнее сообщение успевает попасть в очередь
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Watch did not finish")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) > 10 {
		t.Fatalf("delivered %d messages, want few (updates are coalesced)", len(got))
	}
	if _, ok := got[len(got)-1].(FinishedMsg); !ok {
		t.Fatalf("last message = %#v, want FinishedMsg", got[len(got)-1])
	}
}
