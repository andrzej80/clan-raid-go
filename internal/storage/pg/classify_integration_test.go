package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

// Классы настоящих ошибок Postgres, которые возвращает HitsProcessor: от них зависит, уйдёт ли
// запись в DLQ (этап 11, A2).
func TestClassifyError_RealPostgresErrors(t *testing.T) {
	t.Parallel()

	env := newProcessorEnv(t, 1)
	r := env.newRaidWithHP(t, 1_000_000)

	t.Run("hit with created_at out of the Postgres range is permanent", func(t *testing.T) {
		// Декодер такую запись пропускает (created_at задан), а база не может её сохранить.
		bad := env.hit(r.ID, env.players[0], 100)
		bad.CreatedAt = time.Date(-10000, 1, 1, 0, 0, 0, 0, time.UTC)
		_, err := env.processor.ProcessBatch(context.Background(), []domain.Hit{bad})
		if err == nil {
			t.Fatal("want an error for created_at out of range")
		}
		if got := pg.ClassifyError(err); got != domain.ErrorPermanent {
			t.Fatalf("ClassifyError(%v) = %v, want permanent", err, got)
		}
	})

	t.Run("statement cut by the attempt deadline is transient", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := env.pool.Exec(ctx, "SELECT pg_sleep(5)")
		if err == nil {
			t.Fatal("want the deadline to cut the statement")
		}
		if got := pg.ClassifyError(err); got != domain.ErrorTransient {
			t.Fatalf("ClassifyError(%v) = %v, want transient", err, got)
		}
	})

	t.Run("missing table after a bad release is unknown, not permanent", func(t *testing.T) {
		_, err := env.pool.Exec(context.Background(), "SELECT 1 FROM table_from_a_missing_migration")
		if err == nil {
			t.Fatal("want undefined table")
		}
		if got := pg.ClassifyError(err); got != domain.ErrorUnknown {
			t.Fatalf("ClassifyError(%v) = %v, want unknown", err, got)
		}
	})
}
