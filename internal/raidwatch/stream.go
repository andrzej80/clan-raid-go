package raidwatch

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"strconv"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	defaultBackoffMin = 200 * time.Millisecond
	defaultBackoffMax = 10 * time.Second
	playerIDHeader    = "x-player-id"
)

// errStreamClosed — сервер закрыл стрим без finished (например, перезапуск api).
var errStreamClosed = errors.New("watch stream closed before the raid finished")

// watchClient — то, что нужно читателю от gRPC-клиента; тесты подставляют свой.
type watchClient interface {
	WatchRaid(
		ctx context.Context, in *raidbossV1.WatchRaidRequest, opts ...grpc.CallOption,
	) (grpc.ServerStreamingClient[raidbossV1.WatchRaidResponse], error)
}

// WatchConfig — параметры читателя стрима.
type WatchConfig struct {
	RaidID   string
	PlayerID int64
	// BackoffMin и BackoffMax ограничивают паузу между переподключениями (по умолчанию 200 мс и 10 с).
	BackoffMin, BackoffMax time.Duration
	// Rand возвращает число из [0, 1) для джиттера; по умолчанию math/rand/v2 (подменяется в тестах).
	Rand func() float64
}

func (c *WatchConfig) setDefaults() {
	if c.BackoffMin <= 0 {
		c.BackoffMin = defaultBackoffMin
	}
	if c.BackoffMax < c.BackoffMin {
		c.BackoffMax = max(defaultBackoffMax, c.BackoffMin)
	}
	if c.Rand == nil {
		c.Rand = rand.Float64 //nolint:gosec // джиттер, не криптография
	}
}

// Watch читает WatchRaid и отдаёт сообщения в send (обычно Program.Send), пока рейд не закончится,
// не случится ошибка, после которой повтор бесполезен, или не отменится ctx. Обрыв стрима не
// завершает работу: Watch переподключается с экспоненциальным backoff и джиттером, а новый стрим
// снова начинается с snapshot.
//
// send вызывается из отдельной горутины, а читатель стрима кладёт сообщения в mailbox и не ждёт UI.
// Watch возвращается, когда обе горутины завершились: после отмены ctx ничего не остаётся работать.
func Watch(ctx context.Context, client watchClient, cfg WatchConfig, send func(tea.Msg)) {
	cfg.setDefaults()
	box := newMailbox()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			msg, ok := box.next()
			if !ok {
				return
			}
			send(msg)
		}
	}()

	watchLoop(ctx, client, cfg, box)
	box.close()
	<-done
}

func watchLoop(ctx context.Context, client watchClient, cfg WatchConfig, box *mailbox) {
	ctx = metadata.AppendToOutgoingContext(ctx, playerIDHeader, strconv.FormatInt(cfg.PlayerID, 10))
	backoff := cfg.BackoffMin
	for {
		progressed, finished, err := watchOnce(ctx, client, cfg.RaidID, box)
		if ctx.Err() != nil || finished {
			return
		}
		if isFatal(err) {
			box.put(ErrorMsg{Err: err})
			return
		}

		box.put(ConnMsg{State: ConnReconnecting})
		if progressed {
			backoff = cfg.BackoffMin // соединение было рабочим: новая серия обрывов начинается с минимума
		}
		timer := time.NewTimer(jitter(backoff, cfg.Rand))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, cfg.BackoffMax)
	}
}

// jitter возвращает паузу из [d/2, d]: клиенты, потерявшие сервер в одну секунду, не вернутся синхронно.
func jitter(d time.Duration, rnd func() float64) time.Duration {
	return d/2 + time.Duration(rnd()*float64(d/2))
}

// isFatal: ошибки, которые повтор не исправит (нет такого рейда, плохой id, нет прав).
func isFatal(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated:
		return true
	}
	return false
}

// watchOnce открывает один стрим и читает его до конца. progressed — получен ли snapshot,
// finished — пришло ли сообщение finished (рейд закончился).
func watchOnce(ctx context.Context, client watchClient, raidID string, box *mailbox) (progressed, finished bool, _ error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // отпускает стрим при любом выходе

	stream, err := client.WatchRaid(ctx, &raidbossV1.WatchRaidRequest{RaidId: raidID})
	if err != nil {
		return false, false, err
	}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return progressed, false, errStreamClosed
		}
		if err != nil {
			return progressed, false, err
		}
		msg := toMsg(resp)
		if msg == nil {
			continue // сообщение неизвестного вида из новой версии сервера
		}
		box.put(msg)
		switch msg.(type) {
		case SnapshotMsg:
			progressed = true
		case FinishedMsg:
			return progressed, true, nil
		}
	}
}

// toMsg переводит сообщение стрима в сообщение модели; nil — сообщение неизвестного вида.
func toMsg(resp *raidbossV1.WatchRaidResponse) tea.Msg {
	switch u := resp.GetUpdate().(type) {
	case *raidbossV1.WatchRaidResponse_Snapshot:
		r := u.Snapshot
		return SnapshotMsg{BossName: r.GetBossName(), HPMax: r.GetHpMax(), HPLeft: r.GetHpLeft(), Status: r.GetStatus()}
	case *raidbossV1.WatchRaidResponse_HpLeft:
		return HPMsg{HPLeft: u.HpLeft}
	case *raidbossV1.WatchRaidResponse_HitsTotal:
		return HitsMsg{HitsTotal: u.HitsTotal}
	case *raidbossV1.WatchRaidResponse_Leaderboard:
		entries := make([]Entry, 0, len(u.Leaderboard.GetEntries()))
		for _, e := range u.Leaderboard.GetEntries() {
			entries = append(entries, Entry{PlayerID: e.GetPlayerId(), Damage: e.GetTotalDamage()})
		}
		return LeaderboardMsg{Entries: entries}
	case *raidbossV1.WatchRaidResponse_Finished:
		return FinishedMsg{Status: u.Finished.GetStatus(), KillerPlayerID: u.Finished.GetKillerPlayerId()}
	}
	return nil
}
