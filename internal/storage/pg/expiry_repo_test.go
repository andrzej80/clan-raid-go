package pg_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/scheduler"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

// Тесты scheduler не параллельные: ExpireOnce забирает просроченные рейды всей базы, и параллельный
// проход соседнего теста увёл бы чужой рейд раньше, чем его проверят.

func newTestScheduler(t *testing.T, env *processorEnv) *scheduler.Scheduler {
	t.Helper()

	s, err := scheduler.New(pg.NewTxManager(env.pool), pg.NewExpiryRepo(env.pool), pg.NewOutboxRepo(env.pool),
		scheduler.Config{Interval: time.Hour, EventsTopic: eventsTopic})
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return s
}

// makeDue сдвигает конец рейда в прошлое: рейд просрочен, но статус не менялся.
func (e *processorEnv) makeDue(t *testing.T, raidID uuid.UUID) {
	t.Helper()
	e.exec(t, `UPDATE raids SET ends_at = now() - interval '1 minute' WHERE id = $1`, raidID)
}

func (e *processorEnv) raidStatus(t *testing.T, raidID uuid.UUID) domain.RaidStatus {
	t.Helper()
	r, err := e.raids.GetRaid(context.Background(), raidID)
	if err != nil {
		t.Fatalf("GetRaid: %v", err)
	}
	return r.Status
}

func (e *processorEnv) outboxEvents(t *testing.T, raidID uuid.UUID) int {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM outbox WHERE topic = $1 AND key = $2`, eventsTopic, raidID.String())
}

func TestScheduler_ExpiresOnlyDueActiveRaids(t *testing.T) { //nolint:paralleltest // см. комментарий выше
	env := newProcessorEnv(t, 1)
	due := env.newRaidWithHP(t, 100)
	notDue := env.newRaidWithHP(t, 100)
	defeated := defeatedRaid(t, newProcessorEnv(t, 2))
	mustProcess(t, env.processor, []domain.Hit{env.hit(due.ID, env.players[0], 30)}) // до просрочки
	env.makeDue(t, due.ID)
	env.makeDue(t, defeated.ID) // убитый рейд с прошедшим ends_at expired становиться не должен

	s := newTestScheduler(t, env)
	if _, err := s.ExpireOnce(context.Background()); err != nil {
		t.Fatalf("ExpireOnce: %v", err)
	}

	if st := env.raidStatus(t, due.ID); st != domain.RaidExpired {
		t.Fatalf("due raid status = %s, want expired", st)
	}
	got, _ := env.raids.GetRaid(context.Background(), due.ID)
	if got.FinishedAt == nil || got.HPLeft != 70 {
		t.Fatalf("expired raid: finished_at=%v hp_left=%d, want set and 70", got.FinishedAt, got.HPLeft)
	}
	if st := env.raidStatus(t, notDue.ID); st != domain.RaidActive {
		t.Fatalf("raid that is not due: status = %s, want active", st)
	}
	if st := env.raidStatus(t, defeated.ID); st != domain.RaidDefeated {
		t.Fatalf("defeated raid status = %s, want defeated", st)
	}

	var payload []byte
	if err := env.pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE topic = $1 AND key = $2`, eventsTopic, due.ID.String()).
		Scan(&payload); err != nil {
		t.Fatalf("outbox: %v", err)
	}
	var ev eventsV1.RaidEvent
	if err := proto.Unmarshal(payload, &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ev.GetExpired() == nil || ev.GetExpired().GetHpLeft() != 70 {
		t.Fatalf("event = %+v, want expired with hp_left 70", &ev)
	}
	if n := env.outboxEvents(t, notDue.ID); n != 0 {
		t.Fatalf("events for raid that is not due = %d, want 0", n)
	}
	// У убитого рейда одно событие (defeated, от консьюмера), expired не добавлен.
	if n := env.outboxEvents(t, defeated.ID); n != 1 {
		t.Fatalf("events for defeated raid = %d, want only the defeated one", n)
	}

	// Повторный проход ничего не делает.
	if _, err := s.ExpireOnce(context.Background()); err != nil {
		t.Fatalf("second ExpireOnce: %v", err)
	}
	if n := env.outboxEvents(t, due.ID); n != 1 {
		t.Fatalf("events after second pass = %d, want 1", n)
	}
}

// Несколько экземпляров scheduler одновременно: событие по рейду создаётся ровно одно.
func TestScheduler_ConcurrentPassesExpireOnce(t *testing.T) { //nolint:paralleltest // см. комментарий выше
	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 100)
	env.makeDue(t, r.ID)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		s := newTestScheduler(t, env)
		wg.Go(func() {
			_, err := s.ExpireOnce(context.Background())
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ExpireOnce: %v", err)
		}
	}

	if st := env.raidStatus(t, r.ID); st != domain.RaidExpired {
		t.Fatalf("status = %s, want expired", st)
	}
	if n := env.outboxEvents(t, r.ID); n != 1 {
		t.Fatalf("expired events = %d, want exactly 1", n)
	}
}

// Проход, который ждёт консьюмера ударов, перепроверяет условие: рейд, добитый последним ударом, остаётся defeated.
func TestScheduler_WaitsForConsumerAndKeepsDefeatedRaid(t *testing.T) { //nolint:paralleltest // см. комментарий выше
	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 100)
	env.makeDue(t, r.ID)
	// Удар создан до ends_at, но рейд ещё active: консьюмер его засчитает и убьёт босса.
	hit := env.hit(r.ID, env.players[0], 100)
	hit.CreatedAt = time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Microsecond)

	hits := pg.NewHitsRepo(env.pool)
	tx := pg.NewTxManager(env.pool)
	passDone := make(chan error, 1)
	err := tx.WithinTx(context.Background(), func(ctx context.Context) error {
		if _, err := hits.LockRaid(ctx, r.ID); err != nil { // консьюмер держит строку рейда
			return err
		}
		s := newTestScheduler(t, env)
		go func() { //nolint:contextcheck // проход scheduler идёт на другом соединении: контекст без транзакции консьюмера
			_, err := s.ExpireOnce(context.Background())
			passDone <- err
		}()
		select {
		case err := <-passDone:
			t.Fatalf("scheduler pass finished while the raid row is locked: %v", err)
		case <-time.After(300 * time.Millisecond): // проход ждёт блокировку
		}
		return hits.UpdateHP(ctx, r.ID, 0, &domain.Kill{PlayerID: env.players[0], HitID: hit.ID, FinishedAt: time.Now()})
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if err := <-passDone; err != nil {
		t.Fatalf("ExpireOnce: %v", err)
	}

	if st := env.raidStatus(t, r.ID); st != domain.RaidDefeated {
		t.Fatalf("status = %s, want defeated: expiry must not overwrite a kill", st)
	}
	if n := env.outboxEvents(t, r.ID); n != 0 {
		t.Fatalf("expired events = %d, want 0", n)
	}
}
