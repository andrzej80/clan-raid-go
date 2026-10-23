package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func TestHitsRepo_LockRaidNotFound(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	tx := pg.NewTxManager(pool)
	repo := pg.NewHitsRepo(pool)

	err := tx.WithinTx(context.Background(), func(ctx context.Context) error {
		_, err := repo.LockRaid(ctx, uuid.New())
		return err
	})
	if !errors.Is(err, domain.ErrRaidNotFound) {
		t.Fatalf("LockRaid error = %v, want ErrRaidNotFound", err)
	}
}

// applyBatch повторяет шаги 6.2 консьюмера поверх репозиториев; вызывается внутри транзакции.
func applyBatch(ctx context.Context, t *testing.T, hitsRepo *pg.HitsRepo, outbox *pg.OutboxRepo,
	raidID uuid.UUID, batch []domain.Hit, now time.Time,
) ([]domain.PlayerDamage, error) {
	t.Helper()

	state, err := hitsRepo.LockRaid(ctx, raidID)
	if err != nil {
		return nil, err
	}
	if state.Status != domain.RaidActive || state.HPLeft != 100 {
		t.Errorf("state = %+v", state)
	}

	ids := make([]uuid.UUID, len(batch))
	for i, h := range batch {
		ids[i] = h.ID
	}
	existing, err := hitsRepo.ExistingHitIDs(ctx, ids)
	if err != nil || len(existing) != 0 {
		t.Errorf("ExistingHitIDs = %v, %v; want empty", existing, err)
	}

	out := domain.ApplyHits(state.HPLeft, batch)
	if out.Killing == nil || len(out.Late) != 1 {
		t.Fatalf("outcome = %+v", out)
	}
	if err := hitsRepo.InsertHits(ctx, out.Accepted); err != nil {
		return nil, err
	}
	totals, err := hitsRepo.AddDamage(ctx, raidID, domain.AggregateDamage(out.Accepted))
	if err != nil {
		return nil, err
	}
	kill := &domain.Kill{PlayerID: out.Killing.PlayerID, HitID: out.Killing.ID, FinishedAt: now}
	if err := hitsRepo.UpdateHP(ctx, raidID, out.HPLeft, kill); err != nil {
		return nil, err
	}
	return totals, outbox.Add(ctx, domain.OutboxMessage{
		Topic: "raid.events", Key: raidID.String(), Payload: []byte{1, 2, 3},
		Headers: map[string]string{"type": "RaidDefeated"},
	})
}

// Полный путь обработки одной пачки: блокировка, фильтр дублей, вставка, агрегат, убийство, outbox.
func TestHitsRepo_ApplyBatchFlow(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	ctx := context.Background()
	txm := pg.NewTxManager(pool)
	raids := pg.NewRaidRepo(pool)
	hitsRepo := pg.NewHitsRepo(pool)
	outbox := pg.NewOutboxRepo(pool)

	clanID := createClan(t, pool)
	p1, _ := createPlayer(t, pool, clanID, 500)
	p2, _ := createPlayer(t, pool, clanID, 500)

	raid := newRaid(clanID)
	raid.HPMax, raid.HPLeft = 100, 100
	if err := raids.CreateRaid(ctx, raid); err != nil {
		t.Fatalf("CreateRaid: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	mk := func(player int64, dmg int) domain.Hit {
		return domain.Hit{ID: uuid.New(), RaidID: raid.ID, PlayerID: player, ClanID: clanID, Damage: dmg, CreatedAt: now}
	}
	batch := []domain.Hit{mk(p1, 40), mk(p2, 30), mk(p1, 50), mk(p2, 10)} // убийство на третьем ударе

	var totals []domain.PlayerDamage
	err := txm.WithinTx(ctx, func(ctx context.Context) error {
		var err error
		totals, err = applyBatch(ctx, t, hitsRepo, outbox, raid.ID, batch, now)
		return err
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}

	sums := map[int64]int64{}
	for _, d := range totals {
		sums[d.PlayerID] = d.Damage
	}
	if sums[p1] != 90 || sums[p2] != 30 {
		t.Fatalf("totals = %v, want p1=90 p2=30", sums)
	}

	got, err := raids.GetRaid(ctx, raid.ID)
	if err != nil {
		t.Fatalf("GetRaid: %v", err)
	}
	if got.Status != domain.RaidDefeated || got.HPLeft != 0 || got.FinishedAt == nil ||
		got.KillerPlayerID == nil || *got.KillerPlayerID != p1 ||
		got.KillerHitID == nil || *got.KillerHitID != batch[2].ID {
		t.Fatalf("raid after kill: %+v", got)
	}

	var outboxCount int
	var headerType string
	if err := pool.QueryRow(ctx,
		`SELECT count(*), coalesce(max(headers->>'type'), '') FROM outbox WHERE key = $1`, raid.ID.String(),
	).Scan(&outboxCount, &headerType); err != nil || outboxCount != 1 || headerType != "RaidDefeated" {
		t.Fatalf("outbox: count=%d type=%q err=%v", outboxCount, headerType, err)
	}

	// Повтор: удары уже записаны, фильтр обязан их найти.
	ids := []uuid.UUID{batch[0].ID, batch[1].ID, batch[3].ID}
	existing, err := hitsRepo.ExistingHitIDs(ctx, ids)
	if err != nil {
		t.Fatalf("ExistingHitIDs: %v", err)
	}
	if _, ok := existing[batch[0].ID]; !ok || len(existing) != 2 { // batch[3] опоздал и не записан
		t.Fatalf("existing = %v", existing)
	}
}

func TestHitsRepo_AddDamageAccumulates(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	ctx := context.Background()
	hitsRepo := pg.NewHitsRepo(pool)

	clanID := createClan(t, pool)
	player, _ := createPlayer(t, pool, clanID, 300)
	raid := newRaid(clanID)
	if err := pg.NewRaidRepo(pool).CreateRaid(ctx, raid); err != nil {
		t.Fatalf("CreateRaid: %v", err)
	}

	for i, want := range []int64{100, 250} {
		delta := []domain.PlayerDamage{{PlayerID: player, Damage: map[int]int64{0: 100, 1: 150}[i], Hits: 1}}
		totals, err := hitsRepo.AddDamage(ctx, raid.ID, delta)
		if err != nil {
			t.Fatalf("AddDamage #%d: %v", i, err)
		}
		if len(totals) != 1 || totals[0].Damage != want || totals[0].Hits != i+1 {
			t.Fatalf("AddDamage #%d totals = %+v, want damage %d hits %d", i, totals, want, i+1)
		}
	}
}

func TestHitsRepo_InsertHitsRejectsDuplicate(t *testing.T) {
	t.Parallel()

	pool := requirePool(t)
	ctx := context.Background()
	hitsRepo := pg.NewHitsRepo(pool)

	clanID := createClan(t, pool)
	player, _ := createPlayer(t, pool, clanID, 300)
	raid := newRaid(clanID)
	if err := pg.NewRaidRepo(pool).CreateRaid(ctx, raid); err != nil {
		t.Fatalf("CreateRaid: %v", err)
	}

	hit := domain.Hit{ID: uuid.New(), RaidID: raid.ID, PlayerID: player, Damage: 10, CreatedAt: time.Now()}
	if err := hitsRepo.InsertHits(ctx, []domain.Hit{hit}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := hitsRepo.InsertHits(ctx, []domain.Hit{hit}); err == nil {
		t.Fatal("duplicate hit_id must fail, got nil")
	}
}
