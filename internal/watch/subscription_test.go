package watch

import (
	"context"
	"errors"
	"testing"
)

func TestSubscription_FirstOverflowGoesToPending(t *testing.T) {
	t.Parallel()
	s := newSubscription(2)
	for i := range 3 {
		s.offer(hpUpdate(int64(i)))
	}
	// 0 и 1 в буфере, 2 ждёт в pending и не теряется: клиент не отключён.
	if got := []int64{nextHP(t, s), nextHP(t, s), nextHP(t, s)}; got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("got %v, want [0 1 2]", got)
	}
}

func TestSubscription_RepeatedOverflowKillsSlowConsumer(t *testing.T) {
	t.Parallel()
	s := newSubscription(1)
	s.offer(hpUpdate(1)) // буфер
	s.offer(hpUpdate(2)) // pending (первое переполнение)
	s.offer(hpUpdate(3)) // повторное переполнение

	if _, err := s.Next(t.Context()); !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("Next err = %v, want ErrSlowConsumer", err)
	}
}

func TestSubscription_ReadingFreesPendingSoNoKill(t *testing.T) {
	t.Parallel()
	s := newSubscription(1)
	s.offer(hpUpdate(1))
	s.offer(hpUpdate(2)) // pending
	if got := nextHP(t, s); got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
	// pending перенесён в буфер при чтении, поэтому следующее обновление снова уходит в pending.
	s.offer(hpUpdate(3))
	if got := nextHP(t, s); got != 2 {
		t.Fatalf("got %d, want 2", got)
	}
	if got := nextHP(t, s); got != 3 {
		t.Fatalf("got %d, want 3", got)
	}
}

func TestSubscription_FinishedReplacesPendingAndNeverKills(t *testing.T) {
	t.Parallel()
	s := newSubscription(1)
	s.offer(hpUpdate(1))
	s.offer(hpUpdate(2)) // pending
	s.offer(finishedUpdate())
	s.offer(hpUpdate(0)) // после finished pending не вытесняется и клиент не отключается

	if got := nextHP(t, s); got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
	if u := nextUpdate(t, s); u.GetFinished() == nil {
		t.Fatalf("got %v, want finished", u)
	}
}

func TestSubscription_CloseStopsNext(t *testing.T) {
	t.Parallel()
	s := newSubscription(4)
	s.offer(hpUpdate(1))
	s.Close()
	s.Close() // идемпотентен

	if _, err := s.Next(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next err = %v, want ErrClosed", err)
	}
}

func TestSubscription_NextHonorsContext(t *testing.T) {
	t.Parallel()
	s := newSubscription(4)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := s.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next err = %v, want context.Canceled", err)
	}
}
