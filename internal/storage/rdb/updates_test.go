package rdb_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
)

func newUpdates(t *testing.T) *rdb.Updates {
	t.Helper()
	if testRedis == nil {
		t.Skip("docker is not available")
	}
	u := rdb.NewUpdates(testRedis)
	t.Cleanup(u.Close)
	return u
}

func TestUpdates_DeliversPublishedUpdatesInOrder(t *testing.T) {
	t.Parallel()
	u, b, raidID := newUpdates(t), newBoard(t), uuid.New()
	got := make(chan *raidbossV1.WatchRaidResponse, 8)

	stop, ready := u.Start(raidID, func(update *raidbossV1.WatchRaidResponse) { got <- update })
	defer stop()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("source is not ready")
	}

	if err := b.Apply(t.Context(), raidID, []domain.PlayerDamage{dmg(1, 100)}, 900, 10); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := b.PublishDefeated(t.Context(), raidID, 1); err != nil {
		t.Fatalf("PublishDefeated: %v", err)
	}

	for i, check := range []func(*raidbossV1.WatchRaidResponse) bool{
		func(u *raidbossV1.WatchRaidResponse) bool { return u.GetHpLeft() == 900 },
		func(u *raidbossV1.WatchRaidResponse) bool { return u.GetHitsTotal() == 10 },
		func(u *raidbossV1.WatchRaidResponse) bool { return len(u.GetLeaderboard().GetEntries()) == 1 },
		func(u *raidbossV1.WatchRaidResponse) bool { return u.GetFinished() != nil },
	} {
		select {
		case update := <-got:
			if !check(update) {
				t.Fatalf("update %d = %v", i, update)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("update %d not delivered", i)
		}
	}
}

func TestUpdates_StopEndsDelivery(t *testing.T) {
	t.Parallel()
	u, b, raidID := newUpdates(t), newBoard(t), uuid.New()
	got := make(chan *raidbossV1.WatchRaidResponse, 8)

	stop, ready := u.Start(raidID, func(update *raidbossV1.WatchRaidResponse) { got <- update })
	<-ready
	stop()
	u.Close() // дожидается горутины: после него sink не вызывается

	_ = b.PublishExpired(t.Context(), raidID)
	select {
	case update := <-got:
		t.Fatalf("got %v after stop", update)
	case <-time.After(100 * time.Millisecond):
	}
}
