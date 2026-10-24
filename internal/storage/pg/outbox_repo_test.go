package pg_test

import (
	"context"
	"slices"
	"testing"

	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

// Эти тесты не параллельные: они берут «все неотправленные» записи общей таблицы outbox,
// а параллельные тесты процессора ударов их добавляют. Перед проверкой накопленное помечается отправленным.
func resetOutbox(t *testing.T) {
	t.Helper()
	if _, err := requirePool(t).Exec(context.Background(),
		`UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL`); err != nil {
		t.Fatalf("reset outbox: %v", err)
	}
}

func addOutbox(t *testing.T, repo *pg.OutboxRepo, key string, n int) {
	t.Helper()
	for i := range n {
		err := repo.Add(context.Background(), domain.OutboxMessage{
			Topic: "raid.events", Key: key, Payload: []byte{byte(i)}, Headers: map[string]string{"type": "T"},
		})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
}

func TestOutboxRepo_LockUnsentAndMarkSent(t *testing.T) { //nolint:paralleltest // берёт все неотправленные записи общей таблицы
	pool := requirePool(t)
	repo := pg.NewOutboxRepo(pool)
	tx := pg.NewTxManager(pool)
	ctx := context.Background()

	resetOutbox(t)
	addOutbox(t, repo, t.Name(), 3)

	var ids []int64
	err := tx.WithinTx(ctx, func(ctx context.Context) error {
		entries, err := repo.LockUnsent(ctx, 2)
		if err != nil {
			return err
		}
		if len(entries) != 2 || entries[0].ID >= entries[1].ID {
			t.Fatalf("entries = %+v, want 2 in id order", entries)
		}
		if e := entries[0]; e.Topic != "raid.events" || e.Key != t.Name() ||
			len(e.Payload) != 1 || e.Headers["type"] != "T" {
			t.Fatalf("entry = %+v", e)
		}
		for _, e := range entries {
			ids = append(ids, e.ID)
		}
		return repo.MarkSent(ctx, ids)
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}

	if n, err := repo.CountUnsent(ctx); err != nil || n != 1 {
		t.Fatalf("CountUnsent = %d, %v; want 1", n, err)
	}
	var sent int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE id = ANY($1) AND sent_at IS NOT NULL`, ids).
		Scan(&sent); err != nil || sent != 2 {
		t.Fatalf("sent = %d, %v; want 2", sent, err)
	}
}

// Два relay'я одновременно: заблокированные первым записи второй пропускает и берёт следующие.
func TestOutboxRepo_SkipLocked(t *testing.T) { //nolint:paralleltest // берёт все неотправленные записи общей таблицы
	pool := requirePool(t)
	repo := pg.NewOutboxRepo(pool)
	tx := pg.NewTxManager(pool)

	resetOutbox(t)
	addOutbox(t, repo, t.Name(), 4)

	err := tx.WithinTx(context.Background(), func(first context.Context) error {
		a, err := repo.LockUnsent(first, 2)
		if err != nil || len(a) != 2 {
			t.Fatalf("first LockUnsent = %v, %v", a, err)
		}

		// Вторая транзакция идёт на другом соединении: контекст не содержит первой.
		return tx.WithinTx(context.Background(), func(second context.Context) error { //nolint:contextcheck // нужен контекст без первой транзакции
			b, err := repo.LockUnsent(second, 10)
			if err != nil {
				return err
			}
			if len(b) != 2 {
				t.Fatalf("second LockUnsent returned %d records, want the 2 not locked by the first", len(b))
			}
			all := []int64{a[0].ID, a[1].ID, b[0].ID, b[1].ID}
			slices.Sort(all)
			if len(slices.Compact(all)) != 4 {
				t.Fatalf("relays got overlapping records: %v", all)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
}

// Откат транзакции не помечает записи отправленными.
func TestOutboxRepo_RollbackKeepsRecordsUnsent(t *testing.T) { //nolint:paralleltest // берёт все неотправленные записи общей таблицы
	pool := requirePool(t)
	repo := pg.NewOutboxRepo(pool)
	tx := pg.NewTxManager(pool)
	ctx := context.Background()

	resetOutbox(t)
	addOutbox(t, repo, t.Name(), 2)

	_ = tx.WithinTx(ctx, func(ctx context.Context) error {
		entries, err := repo.LockUnsent(ctx, 10)
		if err != nil {
			return err
		}
		if err := repo.MarkSent(ctx, []int64{entries[0].ID, entries[1].ID}); err != nil {
			return err
		}
		return context.Canceled // откат
	})

	if n, err := repo.CountUnsent(ctx); err != nil || n != 2 {
		t.Fatalf("CountUnsent after rollback = %d, %v; want 2", n, err)
	}
}
