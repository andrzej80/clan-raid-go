package rdb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/rdb"
	"github.com/andrzej80/clan-raid-go/internal/testutil"
)

// testRedis общий для тестов пакета; nil, если Docker недоступен.
var testRedis *redis.Client

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	client, cleanup, err := testutil.StartRedis(context.Background())
	if err != nil {
		// В CI Docker обязателен: иначе интеграционные тесты молча пропустятся, а сборка останется зелёной.
		if os.Getenv("REQUIRE_DOCKER") == "true" {
			fmt.Fprintln(os.Stderr, "redis unavailable, but REQUIRE_DOCKER=true:", err)
			return 1
		}
		fmt.Fprintln(os.Stderr, "redis unavailable, integration tests will be skipped:", err)
		return m.Run()
	}
	defer cleanup()
	testRedis = client
	return m.Run()
}

func newBoard(t *testing.T) *rdb.Board {
	t.Helper()
	if testRedis == nil {
		t.Skip("docker is not available")
	}
	return rdb.NewBoard(testRedis)
}

func dmg(player, total int64) domain.PlayerDamage {
	return domain.PlayerDamage{PlayerID: player, Damage: total, Hits: 1}
}

func TestBoard_ApplyAndTop(t *testing.T) {
	t.Parallel()
	b, raidID := newBoard(t), uuid.New()

	if err := b.Apply(context.Background(), raidID, []domain.PlayerDamage{dmg(1, 100), dmg(2, 300), dmg(3, 200)}, 400, 10); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, err := b.Top(context.Background(), raidID, 2)
	if err != nil {
		t.Fatalf("Top: %v", err)
	}
	want := []domain.LeaderboardEntry{{PlayerID: 2, TotalDamage: 300}, {PlayerID: 3, TotalDamage: 200}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("top = %+v, want %+v", got, want)
	}

	hp, err := testRedis.Get(context.Background(), rdb.HPKey(raidID)).Int64()
	if err != nil || hp != 400 {
		t.Fatalf("hp = %d, err = %v, want 400", hp, err)
	}
}

// Повторная обработка той же пачки (после падения консьюмера) не должна менять состояние:
// ZADD записывает абсолютные значения, а не прибавляет.
func TestBoard_ApplyIsIdempotent(t *testing.T) {
	t.Parallel()
	b, raidID := newBoard(t), uuid.New()
	totals := []domain.PlayerDamage{dmg(1, 100), dmg(2, 300)}

	for range 3 {
		if err := b.Apply(context.Background(), raidID, totals, 600, 10); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	got, err := b.Top(context.Background(), raidID, 10)
	if err != nil {
		t.Fatalf("Top: %v", err)
	}
	if len(got) != 2 || got[0].TotalDamage != 300 || got[1].TotalDamage != 100 {
		t.Fatalf("top = %+v, want totals unchanged after repeated Apply", got)
	}
}

func TestBoard_ApplyOverwritesWithNewerTotals(t *testing.T) {
	t.Parallel()
	b, raidID := newBoard(t), uuid.New()

	_ = b.Apply(context.Background(), raidID, []domain.PlayerDamage{dmg(1, 100)}, 900, 10)
	_ = b.Apply(context.Background(), raidID, []domain.PlayerDamage{dmg(1, 250)}, 750, 10)

	got, _ := b.Top(context.Background(), raidID, 10)
	if len(got) != 1 || got[0].TotalDamage != 250 {
		t.Fatalf("top = %+v, want player 1 with 250", got)
	}
}

func TestBoard_TopOfUnknownRaidIsEmpty(t *testing.T) {
	t.Parallel()
	b := newBoard(t)

	got, err := b.Top(context.Background(), uuid.New(), 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("top = %+v, err = %v, want empty", got, err)
	}
}

func TestBoard_ApplyPublishesHP(t *testing.T) {
	t.Parallel()
	b, raidID := newBoard(t), uuid.New()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub := testRedis.Subscribe(ctx, rdb.UpdatesChannel(raidID))
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(ctx); err != nil { // подтверждение подписки
		t.Fatalf("subscribe: %v", err)
	}

	if err := b.Apply(ctx, raidID, []domain.PlayerDamage{dmg(1, 100)}, 321, 10); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	var update raidbossV1.WatchRaidResponse
	if err := proto.Unmarshal([]byte(msg.Payload), &update); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if update.GetHpLeft() != 321 {
		t.Fatalf("published hp = %d, want 321", update.GetHpLeft())
	}
}

func receiveUpdate(t *testing.T, ctx context.Context, sub *redis.PubSub) *raidbossV1.WatchRaidResponse {
	t.Helper()
	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	var update raidbossV1.WatchRaidResponse
	if err := proto.Unmarshal([]byte(msg.Payload), &update); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &update
}

func subscribeUpdates(t *testing.T, ctx context.Context, raidID uuid.UUID) *redis.PubSub {
	t.Helper()
	sub := testRedis.Subscribe(ctx, rdb.UpdatesChannel(raidID))
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return sub
}

func TestBoard_ApplyPublishesLeaderboardAfterHP(t *testing.T) {
	t.Parallel()
	b, raidID := newBoard(t), uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub := subscribeUpdates(t, ctx, raidID)

	if err := b.Apply(ctx, raidID, []domain.PlayerDamage{dmg(1, 100), dmg(2, 300)}, 600, 10); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if hp := receiveUpdate(t, ctx, sub); hp.GetHpLeft() != 600 {
		t.Fatalf("first update = %v, want hp_left=600", hp)
	}
	if hits := receiveUpdate(t, ctx, sub); hits.GetHitsTotal() != 10 {
		t.Fatalf("second update = %v, want hits_total=10", hits)
	}
	entries := receiveUpdate(t, ctx, sub).GetLeaderboard().GetEntries()
	if len(entries) != 2 || entries[0].GetPlayerId() != 2 || entries[0].GetTotalDamage() != 300 ||
		entries[1].GetPlayerId() != 1 {
		t.Fatalf("leaderboard = %v, want players 2 (300) then 1 (100)", entries)
	}
}

func TestBoard_PublishFinished(t *testing.T) {
	t.Parallel()
	b, raidID := newBoard(t), uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub := subscribeUpdates(t, ctx, raidID)

	if err := b.PublishDefeated(ctx, raidID, 7); err != nil {
		t.Fatalf("PublishDefeated: %v", err)
	}
	if err := b.PublishExpired(ctx, raidID); err != nil {
		t.Fatalf("PublishExpired: %v", err)
	}

	defeated := receiveUpdate(t, ctx, sub).GetFinished()
	if defeated.GetStatus() != raidbossV1.RaidStatus_RAID_STATUS_DEFEATED || defeated.GetKillerPlayerId() != 7 {
		t.Fatalf("defeated = %v", defeated)
	}
	expired := receiveUpdate(t, ctx, sub).GetFinished()
	if expired.GetStatus() != raidbossV1.RaidStatus_RAID_STATUS_EXPIRED {
		t.Fatalf("expired = %v", expired)
	}
}
