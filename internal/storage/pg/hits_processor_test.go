package pg_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/raid"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

const eventsTopic = "raid.events"

type processorEnv struct {
	pool      *pgxpool.Pool
	processor *raid.HitsProcessor
	raids     *pg.RaidRepo
	clanID    int64
	players   []int64
}

func newProcessorEnv(t *testing.T, players int, opts ...raid.ProcessorOption) *processorEnv {
	t.Helper()

	pool := requirePool(t)
	processor, err := raid.NewHitsProcessor(
		pg.NewTxManager(pool), pg.NewHitsRepo(pool), pg.NewOutboxRepo(pool), eventsTopic, opts...)
	if err != nil {
		t.Fatalf("NewHitsProcessor: %v", err)
	}
	env := &processorEnv{pool: pool, processor: processor, raids: pg.NewRaidRepo(pool), clanID: createClan(t, pool)}
	for range players {
		id, _ := createPlayer(t, pool, env.clanID, 500)
		env.players = append(env.players, id)
	}
	return env
}

// newRaidWithHP создаёт активный рейд; клан у каждого рейда свой, поэтому в одном тесте их несколько.
func (e *processorEnv) newRaidWithHP(t *testing.T, hp int64) *domain.Raid {
	t.Helper()

	r := newRaid(createClan(t, e.pool))
	r.HPMax, r.HPLeft = hp, hp
	if err := e.raids.CreateRaid(context.Background(), r); err != nil {
		t.Fatalf("CreateRaid: %v", err)
	}
	return r
}

func (e *processorEnv) hit(raidID uuid.UUID, player int64, damage int) domain.Hit {
	return domain.Hit{
		ID: uuid.New(), RaidID: raidID, PlayerID: player, ClanID: e.clanID, Damage: damage,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
}

func (e *processorEnv) count(t *testing.T, query string, args ...any) int {
	t.Helper()

	var n int
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

func mustProcess(t *testing.T, p *raid.HitsProcessor, batch []domain.Hit) domain.BatchResult {
	t.Helper()

	res, err := p.ProcessBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	return res
}

func TestHitsProcessor_KillingBlowMidBatch(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 2)
	r := env.newRaidWithHP(t, 100)
	batch := []domain.Hit{
		env.hit(r.ID, env.players[0], 40),
		env.hit(r.ID, env.players[1], 30),
		env.hit(r.ID, env.players[0], 50), // killing blow
		env.hit(r.ID, env.players[1], 10), // опоздал
	}

	if res := mustProcess(t, env.processor, batch); len(res.Rejected) != 0 {
		t.Fatalf("rejected = %+v", res.Rejected)
	}

	got, err := env.raids.GetRaid(context.Background(), r.ID)
	if err != nil {
		t.Fatalf("GetRaid: %v", err)
	}
	if got.Status != domain.RaidDefeated || got.HPLeft != 0 || got.FinishedAt == nil ||
		got.KillerPlayerID == nil || *got.KillerPlayerID != env.players[0] ||
		got.KillerHitID == nil || *got.KillerHitID != batch[2].ID {
		t.Fatalf("raid = %+v", got)
	}
	if n := env.count(t, `SELECT count(*) FROM raid_hits WHERE raid_id = $1`, r.ID); n != 3 {
		t.Fatalf("raid_hits = %d, want 3 (late hit must not be stored)", n)
	}
	if n := env.count(t, `SELECT total_damage FROM raid_damage WHERE raid_id = $1 AND player_id = $2`,
		r.ID, env.players[0]); n != 90 {
		t.Fatalf("player 0 total = %d, want 90", n)
	}

	var (
		topic   string
		payload []byte
	)
	if err := env.pool.QueryRow(context.Background(),
		`SELECT topic, payload FROM outbox WHERE key = $1`, r.ID.String()).Scan(&topic, &payload); err != nil {
		t.Fatalf("outbox: %v", err)
	}
	var event eventsV1.RaidEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		t.Fatalf("unmarshal outbox payload: %v", err)
	}
	if topic != eventsTopic || event.GetRaidId() != r.ID.String() ||
		event.GetDefeated().GetKillerHitId() != batch[2].ID.String() ||
		event.GetDefeated().GetKillerPlayerId() != env.players[0] {
		t.Fatalf("outbox event: topic=%q %+v", topic, &event)
	}
}

// Консьюмер упал после COMMIT, но до коммита offset'а: та же пачка приходит повторно.
func TestHitsProcessor_ReplayIsIdempotent(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 100)

	partial := []domain.Hit{env.hit(r.ID, env.players[0], 20), env.hit(r.ID, env.players[0], 30)}
	mustProcess(t, env.processor, partial)
	mustProcess(t, env.processor, partial) // повтор без убийства: урон не должен удвоиться

	got, _ := env.raids.GetRaid(context.Background(), r.ID)
	if got.HPLeft != 50 || got.Status != domain.RaidActive {
		t.Fatalf("after replay: hp=%d status=%s, want 50 active", got.HPLeft, got.Status)
	}

	killing := []domain.Hit{env.hit(r.ID, env.players[0], 100)}
	mustProcess(t, env.processor, killing)
	mustProcess(t, env.processor, killing) // повтор после убийства: рейд уже defeated

	if n := env.count(t, `SELECT count(*) FROM outbox WHERE key = $1`, r.ID.String()); n != 1 {
		t.Fatalf("outbox events = %d, want 1", n)
	}
	if n := env.count(t, `SELECT total_damage FROM raid_damage WHERE raid_id = $1`, r.ID); n != 150 {
		t.Fatalf("total_damage = %d, want 150", n)
	}
	if n := env.count(t, `SELECT hits FROM raid_damage WHERE raid_id = $1`, r.ID); n != 3 {
		t.Fatalf("hits = %d, want 3", n)
	}
}

func TestHitsProcessor_DuplicatesInsideBatch(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 1000)
	h := env.hit(r.ID, env.players[0], 100)

	mustProcess(t, env.processor, []domain.Hit{h, h, h})

	got, _ := env.raids.GetRaid(context.Background(), r.ID)
	if got.HPLeft != 900 {
		t.Fatalf("hp = %d, want 900", got.HPLeft)
	}
}

// Две копии («зомби» после ребаланса) обрабатывают одну пачку одновременно.
func TestHitsProcessor_ConcurrentZombies(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 3)
	r := env.newRaidWithHP(t, 1000)
	batch := make([]domain.Hit, 0, 30)
	for i := range 30 {
		batch = append(batch, env.hit(r.ID, env.players[i%3], 10))
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := env.processor.ProcessBatch(context.Background(), batch); err != nil {
				t.Errorf("ProcessBatch: %v", err)
			}
		})
	}
	wg.Wait()

	got, _ := env.raids.GetRaid(context.Background(), r.ID)
	if got.HPLeft != 700 {
		t.Fatalf("hp = %d, want 700", got.HPLeft)
	}
	if n := env.count(t, `SELECT count(*) FROM raid_hits WHERE raid_id = $1`, r.ID); n != 30 {
		t.Fatalf("raid_hits = %d, want 30", n)
	}
	if n := env.count(t, `SELECT sum(total_damage)::int FROM raid_damage WHERE raid_id = $1`, r.ID); n != 300 {
		t.Fatalf("sum damage = %d, want 300", n)
	}
}

func TestHitsProcessor_RejectsUnknownPlayerAndRaid(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	bad := env.newRaidWithHP(t, 100)
	good := env.newRaidWithHP(t, 100)

	batch := []domain.Hit{
		env.hit(bad.ID, 987654321, 10), // игрока не существует
		env.hit(good.ID, env.players[0], 10),
		env.hit(uuid.New(), env.players[0], 10), // рейда не существует
	}
	res := mustProcess(t, env.processor, batch)

	if len(res.Rejected) != 2 || res.Rejected[0].RaidID != bad.ID || len(res.Rejected[0].Hits) != 1 ||
		res.Rejected[1].RaidID != batch[2].RaidID {
		t.Fatalf("rejected = %+v", res.Rejected)
	}
	if n := env.count(t, `SELECT count(*) FROM raid_hits WHERE raid_id = $1`, bad.ID); n != 0 {
		t.Fatalf("bad raid must stay untouched, raid_hits = %d", n)
	}
	gotGood, _ := env.raids.GetRaid(context.Background(), good.ID)
	if gotGood.HPLeft != 90 {
		t.Fatalf("good raid hp = %d, want 90 (rejects must not block other raids)", gotGood.HPLeft)
	}
}

func (e *processorEnv) exec(t *testing.T, query string, args ...any) {
	t.Helper()

	if _, err := e.pool.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
