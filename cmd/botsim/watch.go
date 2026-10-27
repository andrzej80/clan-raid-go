package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	watchBackoffMin = 100 * time.Millisecond
	watchBackoffMax = 5 * time.Second
)

// watchStats — общие счётчики всех подписчиков WatchRaid.
//
// В WatchRaidResponse нет серверной метки времени, поэтому задержку доставки от удара до экрана
// измерить нельзя. Вместо неё считаются паузы между соседними сообщениями одного стрима:
// медленный или залипший стрим виден как рост p99/max пауз.
type watchStats struct {
	snapshots, hpUpdates, leaderboards, finished atomic.Int64
	connects, reconnects                         atomic.Int64
	slowKicked                                   atomic.Int64 // стрим закрыт сервером с RESOURCE_EXHAUSTED
	failures                                     atomic.Int64 // прочие обрывы

	mu   sync.Mutex
	gaps *hdrhistogram.Histogram
}

func newWatchStats() *watchStats {
	return &watchStats{gaps: hdrhistogram.New(int64(time.Microsecond), int64(time.Minute), histDigits)}
}

func (w *watchStats) recordGap(d time.Duration) {
	w.mu.Lock()
	_ = w.gaps.RecordValue(min(max(int64(d), int64(time.Microsecond)), int64(time.Minute))) //nolint:gosec // значение прижато к диапазону
	w.mu.Unlock()
}

// watchReport — итог по подписчикам; попадает в текстовый отчёт и в JSON.
type watchReport struct {
	Connects     int64   `json:"connects"`
	Reconnects   int64   `json:"reconnects"`
	Snapshots    int64   `json:"snapshots"`
	HPUpdates    int64   `json:"hp_updates"`
	Leaderboards int64   `json:"leaderboards"`
	Finished     int64   `json:"finished"`
	SlowKicked   int64   `json:"slow_kicked"`
	Failures     int64   `json:"failures"`
	GapP50Ms     float64 `json:"gap_p50_ms"`
	GapP99Ms     float64 `json:"gap_p99_ms"`
	GapMaxMs     float64 `json:"gap_max_ms"`
}

func (w *watchStats) report() *watchReport {
	w.mu.Lock()
	defer w.mu.Unlock()
	return &watchReport{
		Connects: w.connects.Load(), Reconnects: w.reconnects.Load(),
		Snapshots: w.snapshots.Load(), HPUpdates: w.hpUpdates.Load(), Leaderboards: w.leaderboards.Load(),
		Finished: w.finished.Load(), SlowKicked: w.slowKicked.Load(), Failures: w.failures.Load(),
		GapP50Ms: ms(float64(w.gaps.ValueAtQuantile(50))),
		GapP99Ms: ms(float64(w.gaps.ValueAtQuantile(99))),
		GapMaxMs: ms(float64(w.gaps.Max())),
	}
}

func (r *watchReport) format() string {
	return fmt.Sprintf("watchers:       connects=%d reconnects=%d slow_kicked=%d failures=%d finished=%d\n"+
		"watch messages: snapshot=%d hp=%d leaderboard=%d\n"+
		"watch gap (ms): p50=%.2f p99=%.2f max=%.2f\n",
		r.Connects, r.Reconnects, r.SlowKicked, r.Failures, r.Finished,
		r.Snapshots, r.HPUpdates, r.Leaderboards, r.GapP50Ms, r.GapP99Ms, r.GapMaxMs)
}

// runWatchers запускает n подписчиков, раздавая рейды и игроков по кругу, и ждёт их завершения.
// Подписчик живёт, пока не придёт finished или не отменится ctx.
func runWatchers(ctx context.Context, conn grpc.ClientConnInterface, targets []*target, n int, ws *watchStats) {
	client := raidbossV1.NewRaidServiceClient(conn)
	var wg sync.WaitGroup
	for i := range n {
		t := targets[i%len(targets)]
		wg.Go(func() { watchLoop(ctx, client, t, ws) })
	}
	wg.Wait()
}

// watchLoop держит один стрим, переподключаясь с экспоненциальным backoff и джиттером.
func watchLoop(ctx context.Context, client raidbossV1.RaidServiceClient, t *target, ws *watchStats) {
	backoff := watchBackoffMin
	for {
		progressed, err := watchOnce(withPlayer(ctx, t.p.id), client, t.raidID, ws)
		if ctx.Err() != nil {
			return
		}
		switch code := status.Code(err); {
		case err == nil:
			ws.finished.Add(1)
			return // рейд завершён, сервер закрыл стрим со статусом OK
		case code == codes.NotFound || code == codes.InvalidArgument || code == codes.PermissionDenied:
			ws.failures.Add(1)
			return // повтор ничего не изменит
		case code == codes.ResourceExhausted:
			ws.slowKicked.Add(1)
		default:
			ws.failures.Add(1)
		}

		if progressed {
			backoff = watchBackoffMin
		}
		ws.reconnects.Add(1)
		pause := time.Duration(float64(backoff) * (0.5 + rand.Float64())) //nolint:gosec // джиттер, не криптография
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}
		backoff = min(backoff*2, watchBackoffMax)
	}
}

// watchOnce открывает стрим и читает его до конца. progressed — получено ли хоть одно сообщение.
// nil означает, что сервер закрыл стрим со статусом OK (после finished).
func watchOnce(ctx context.Context, client raidbossV1.RaidServiceClient, raidID string, ws *watchStats) (progressed bool, _ error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // освобождает стрим, если выходим по ошибке

	stream, err := client.WatchRaid(ctx, &raidbossV1.WatchRaidRequest{RaidId: raidID})
	if err != nil {
		return false, err
	}
	ws.connects.Add(1)

	var last time.Time
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return progressed, nil
		}
		if err != nil {
			return progressed, err
		}
		now := time.Now()
		if progressed {
			ws.recordGap(now.Sub(last))
		}
		progressed, last = true, now

		switch msg.GetUpdate().(type) {
		case *raidbossV1.WatchRaidResponse_Snapshot:
			ws.snapshots.Add(1)
		case *raidbossV1.WatchRaidResponse_HpLeft:
			ws.hpUpdates.Add(1)
		case *raidbossV1.WatchRaidResponse_Leaderboard:
			ws.leaderboards.Add(1)
		}
	}
}
