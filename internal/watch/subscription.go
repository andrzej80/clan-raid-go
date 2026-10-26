package watch

import (
	"context"
	"sync"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// Subscription — подписка одного клиента на рейд.
//
// Hub кладёт обновления в буфер без блокировки (offer). Когда буфер заполнен, первое
// обновление ждёт в запасном слоте pending и попадёт в буфер, как только читатель освободит
// место. Если место не освободилось и пришло ещё одно обновление, клиент не читает, и
// подписка закрывается с ErrSlowConsumer. Сообщение finished — последнее, поэтому оно
// вытесняет pending, никогда не приводит к отключению и не теряется.
type Subscription struct {
	ch      chan *Update
	done    chan struct{} // закрывается при kill; ch не закрывается, чтобы offer не паниковал
	release func()        // отписка в hub; задаётся hub'ом до выдачи подписки
	once    sync.Once

	mu      sync.Mutex
	pending *Update
	dead    bool
	err     error
}

func newSubscription(bufferSize int) *Subscription {
	return &Subscription{
		ch:   make(chan *Update, bufferSize),
		done: make(chan struct{}),
	}
}

// Next возвращает следующее обновление. Вызывается из горутины RPC, она же делает stream.Send.
// Ошибка — ErrSlowConsumer, ErrClosed, ErrHubClosed или ошибка ctx.
func (s *Subscription) Next(ctx context.Context) (*Update, error) {
	select {
	case <-s.done:
		return nil, s.err
	default:
	}
	select {
	case u := <-s.ch:
		s.refill()
		return u, nil
	case <-s.done:
		return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Done закрывается, когда подписка завершена (медленный клиент, Close, остановка hub);
// причину возвращает Next.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Close отписывает клиента. Идемпотентен, вызывается через defer в обработчике RPC.
func (s *Subscription) Close() {
	s.kill(ErrClosed)
	s.once.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

// offer кладёт обновление в буфер, не блокируясь.
func (s *Subscription) offer(u *Update) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return
	}
	s.flushPendingLocked()
	if s.pending == nil {
		select {
		case s.ch <- u:
			return
		default:
		}
		s.pending = u // первое переполнение: запасной слот
		return
	}
	// Буфер заполнен и pending занят: повторное переполнение.
	switch {
	case isFinished(s.pending): // последнее сообщение уже ждёт отправки
	case isFinished(u):
		s.pending = u
	default:
		s.killLocked(ErrSlowConsumer)
	}
}

// refill переносит pending в буфер, когда читатель освободил место. Без этого pending
// застрял бы, если новых обновлений больше нет.
func (s *Subscription) refill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushPendingLocked()
}

func (s *Subscription) flushPendingLocked() {
	if s.pending == nil {
		return
	}
	select {
	case s.ch <- s.pending:
		s.pending = nil
	default:
	}
}

func (s *Subscription) kill(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.killLocked(err)
}

func (s *Subscription) killLocked(err error) {
	if s.dead {
		return
	}
	s.dead = true
	s.err = err
	s.pending = nil
	close(s.done)
}

func isFinished(u *Update) bool {
	_, ok := u.GetUpdate().(*raidbossV1.WatchRaidResponse_Finished)
	return ok
}
