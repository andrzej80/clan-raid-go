package watch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// Утечка горутин после тестов (в том числе забытый Close hub'а) валит пакет.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const waitTimeout = 2 * time.Second

// fakeFeeder — источник обновлений для тестов: считает Start/stop и позволяет вбрасывать
// сообщения вручную.
type fakeFeeder struct {
	mu     sync.Mutex
	sinks  map[uuid.UUID]func(*Update)
	starts map[uuid.UUID]int
	stops  map[uuid.UUID]int
}

func newFakeFeeder() *fakeFeeder {
	return &fakeFeeder{
		sinks:  make(map[uuid.UUID]func(*Update)),
		starts: make(map[uuid.UUID]int),
		stops:  make(map[uuid.UUID]int),
	}
}

func (f *fakeFeeder) Start(raidID uuid.UUID, sink func(*Update)) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sinks[raidID] = sink
	f.starts[raidID]++
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.stops[raidID]++
		delete(f.sinks, raidID)
	}
}

// sink возвращает функцию доставки или nil, если источник рейда остановлен.
func (f *fakeFeeder) sink(raidID uuid.UUID) func(*Update) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sinks[raidID]
}

func (f *fakeFeeder) emit(t testing.TB, raidID uuid.UUID, u *Update) {
	t.Helper()
	sink := f.sink(raidID)
	if sink == nil {
		t.Fatalf("source of raid %s is not running", raidID)
	}
	sink(u)
}

func (f *fakeFeeder) counts(raidID uuid.UUID) (starts, stops int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts[raidID], f.stops[raidID]
}

func hpUpdate(hp int64) *Update {
	return &Update{Update: &raidbossV1.WatchRaidResponse_HpLeft{HpLeft: hp}}
}

func leaderboardUpdate() *Update {
	return &Update{Update: &raidbossV1.WatchRaidResponse_Leaderboard{
		Leaderboard: &raidbossV1.GetLeaderboardResponse{},
	}}
}

func finishedUpdate() *Update {
	return &Update{Update: &raidbossV1.WatchRaidResponse_Finished{
		Finished: &raidbossV1.RaidFinished{Status: raidbossV1.RaidStatus_RAID_STATUS_DEFEATED},
	}}
}

// nextUpdate ждёт следующее обновление подписки.
func nextUpdate(t testing.TB, sub *Subscription) *Update {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	u, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return u
}

func nextHP(t testing.TB, sub *Subscription) int64 {
	t.Helper()
	u := nextUpdate(t, sub)
	if _, ok := u.GetUpdate().(*raidbossV1.WatchRaidResponse_HpLeft); !ok {
		t.Fatalf("expected hp_left, got %T", u.GetUpdate())
	}
	return u.GetHpLeft()
}

// expectSilence проверяет, что за короткое время ничего не пришло.
func expectSilence(t testing.TB, sub *Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	if u, err := sub.Next(ctx); err == nil {
		t.Fatalf("unexpected update %v", u)
	}
}
