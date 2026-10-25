package pg_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/rewards"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

const rewardsTopic = "rewards.granted"

func newRewardsProcessor(t *testing.T, env *processorEnv) *rewards.Processor {
	t.Helper()

	p, err := rewards.NewProcessor(
		pg.NewTxManager(env.pool), pg.NewRewardsRepo(env.pool), pg.NewOutboxRepo(env.pool), rewardsTopic)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	return p
}

// defeatedRaid убивает босса через настоящий процессор ударов: игрок 0 бьёт 40 и добивает 50
// (итого 90), игрок 1 бьёт 30. Всего 120 урона.
func defeatedRaid(t *testing.T, env *processorEnv) *domain.Raid {
	t.Helper()

	r := env.newRaidWithHP(t, 100)
	mustProcess(t, env.processor, []domain.Hit{
		env.hit(r.ID, env.players[0], 40),
		env.hit(r.ID, env.players[1], 30),
		env.hit(r.ID, env.players[0], 50), // killing blow
	})
	return r
}

func (e *processorEnv) rewardAmounts(t *testing.T, raidID uuid.UUID) map[string]int {
	t.Helper()

	rows, err := e.pool.Query(context.Background(),
		`SELECT player_id, kind, amount FROM rewards WHERE raid_id = $1`, raidID)
	if err != nil {
		t.Fatalf("select rewards: %v", err)
	}
	defer rows.Close()

	got := map[string]int{}
	for rows.Next() {
		var (
			player int64
			kind   string
			amount int
		)
		if err := rows.Scan(&player, &kind, &amount); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[strconv.FormatInt(player, 10)+"/"+kind] = amount
	}
	return got
}

// rewardEvents считает записи outbox с наградами этих игроков.
func (e *processorEnv) rewardEvents(t *testing.T) int {
	t.Helper()

	keys := make([]string, len(e.players))
	for i, id := range e.players {
		keys[i] = strconv.FormatInt(id, 10)
	}
	return e.count(t, `SELECT count(*) FROM outbox WHERE topic = $1 AND key = ANY($2)`, rewardsTopic, keys)
}

func TestRewardsProcessor_GrantsRewardsForDefeatedRaid(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := defeatedRaid(t, env)
	proc := newRewardsProcessor(t, env)

	rejected, err := proc.ProcessEvents(context.Background(),
		[]domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventDefeated}})
	if err != nil || len(rejected) != 0 {
		t.Fatalf("ProcessEvents = %v, %v; want no rejected and no error", rejected, err)
	}

	p0, p1 := strconv.FormatInt(env.players[0], 10), strconv.FormatInt(env.players[1], 10)
	// Доли урона 90/120 и 30/120: участие 100+750 и 100+250, топ-3 500 и 300, убийце +250.
	want := map[string]int{
		p0 + "/participation": 850, p1 + "/participation": 350,
		p0 + "/top3": 500, p1 + "/top3": 300,
		p0 + "/killing_blow": 250,
	}
	got := env.rewardAmounts(t, r.ID)
	if len(got) != len(want) {
		t.Fatalf("rewards = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("rewards = %v, want %v", got, want)
		}
	}

	if n := env.rewardEvents(t); n != len(want) {
		t.Fatalf("outbox reward events = %d, want %d", n, len(want))
	}
	var payload []byte
	if err := env.pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1 AND key = $2 ORDER BY id LIMIT 1`, rewardsTopic, p0).
		Scan(&payload); err != nil {
		t.Fatalf("outbox payload: %v", err)
	}
	var ev eventsV1.RewardGranted
	if err := proto.Unmarshal(payload, &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.GetRaidId() != r.ID.String() || ev.GetPlayerId() != env.players[0] {
		t.Fatalf("reward event = %+v", &ev)
	}
}

// Повторная доставка события: награды не удваиваются и не порождают новых записей в outbox.
func TestRewardsProcessor_ReplayIsIdempotent(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := defeatedRaid(t, env)
	proc := newRewardsProcessor(t, env)
	events := []domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventDefeated}}

	for range 3 {
		if rejected, err := proc.ProcessEvents(context.Background(), events); err != nil || len(rejected) != 0 {
			t.Fatalf("ProcessEvents = %v, %v", rejected, err)
		}
	}
	if n := len(env.rewardAmounts(t, r.ID)); n != 5 {
		t.Fatalf("rewards = %d after replays, want 5", n)
	}
	if n := env.rewardEvents(t); n != 5 {
		t.Fatalf("outbox reward events = %d after replays, want 5", n)
	}
}

func TestRewardsProcessor_ExpiredEventGrantsNothing(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 100)
	mustProcess(t, env.processor, []domain.Hit{env.hit(r.ID, env.players[0], 10)})
	proc := newRewardsProcessor(t, env)

	rejected, err := proc.ProcessEvents(context.Background(),
		[]domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventExpired}})
	if err != nil || len(rejected) != 0 {
		t.Fatalf("ProcessEvents = %v, %v; want expired event skipped", rejected, err)
	}
	if n := len(env.rewardAmounts(t, r.ID)); n != 0 {
		t.Fatalf("rewards = %d for expired raid, want 0", n)
	}
}

// Расхождение данных (событие defeated по не убитому или несуществующему рейду) уходит в отклонённые
// с индексом события в пачке; остальные события пачки обрабатываются.
func TestRewardsProcessor_RejectsInconsistentEvents(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	good := defeatedRaid(t, env)
	active := env.newRaidWithHP(t, 100)
	proc := newRewardsProcessor(t, env)

	rejected, err := proc.ProcessEvents(context.Background(), []domain.RaidEvent{
		{RaidID: active.ID, Kind: domain.RaidEventDefeated},
		{RaidID: good.ID, Kind: domain.RaidEventDefeated},
		{RaidID: uuid.New(), Kind: domain.RaidEventDefeated},
		{RaidID: good.ID, Kind: "bogus"},
	})
	if err != nil {
		t.Fatalf("ProcessEvents: %v", err)
	}
	indexes := make([]int, len(rejected))
	for i, r := range rejected {
		indexes[i] = r.Index
	}
	if len(indexes) != 3 || indexes[0] != 0 || indexes[1] != 2 || indexes[2] != 3 {
		t.Fatalf("rejected indexes = %v (%+v), want [0 2 3]", indexes, rejected)
	}
	if n := len(env.rewardAmounts(t, active.ID)); n != 0 {
		t.Fatalf("rewards for active raid = %d, want 0", n)
	}
	if n := len(env.rewardAmounts(t, good.ID)); n != 5 {
		t.Fatalf("rewards for defeated raid = %d, want 5", n)
	}
}

// Два воркера обрабатывают одно событие одновременно (ребаланс, «зомби»): награды вставляются один раз.
func TestRewardsProcessor_ConcurrentWorkersGrantOnce(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := defeatedRaid(t, env)
	events := []domain.RaidEvent{{RaidID: r.ID, Kind: domain.RaidEventDefeated}}

	errs := make(chan error, 8)
	for range 8 {
		proc := newRewardsProcessor(t, env)
		go func() {
			_, err := proc.ProcessEvents(context.Background(), events)
			errs <- err
		}()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Fatalf("ProcessEvents: %v", err)
		}
	}
	if n := len(env.rewardAmounts(t, r.ID)); n != 5 {
		t.Fatalf("rewards = %d, want 5", n)
	}
	if n := env.rewardEvents(t); n != 5 {
		t.Fatalf("outbox reward events = %d, want 5: each reward must be announced once", n)
	}
}
