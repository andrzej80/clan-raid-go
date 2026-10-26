package watch

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MutexHub — реализация Hub на sync.RWMutex.
//
// mu защищает карту рейдов и множества подписчиков: раздача обновления идёт под RLock, то
// есть рейды обрабатываются параллельно, а подписка и отписка берут Lock. room.mu
// сериализует accept/flush/broadcast одного рейда, чтобы порядок обновлений не нарушался.
// Блокировки берутся всегда в порядке mu → room.mu → Subscription.mu.
type MutexHub struct {
	cfg    Config
	feeder Feeder

	mu     sync.RWMutex
	rooms  map[uuid.UUID]*mutexRoom
	closed bool
}

var _ Hub = (*MutexHub)(nil)

type mutexRoom struct {
	*room
	mu sync.Mutex
}

func NewMutexHub(feeder Feeder, cfg Config) *MutexHub {
	return &MutexHub{
		cfg:    cfg.withDefaults(),
		feeder: feeder,
		rooms:  make(map[uuid.UUID]*mutexRoom),
	}
}

func (h *MutexHub) Subscribe(ctx context.Context, raidID uuid.UUID) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sub := newSubscription(h.cfg.BufferSize)
	sub.release = func() { h.unsubscribe(raidID, sub) }

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHubClosed
	}
	r := h.rooms[raidID]
	if r == nil {
		r = &mutexRoom{room: newRoom(h.cfg.Interval)}
		r.stopFeed = h.feeder.Start(raidID, func(u *Update) { h.publish(raidID, u) })
		h.rooms[raidID] = r
	}
	r.subs[sub] = struct{}{}
	return sub, nil
}

func (h *MutexHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, r := range h.rooms {
		r.shutdown(ErrHubClosed)
		delete(h.rooms, id)
	}
}

func (h *MutexHub) unsubscribe(raidID uuid.UUID, sub *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.rooms[raidID]
	if r == nil {
		return
	}
	delete(r.subs, sub)
	if len(r.subs) == 0 {
		r.shutdown(nil)
		delete(h.rooms, raidID)
	}
}

// publish вызывается из горутины источника. RLock держится всю раздачу: Lock (отписка
// последнего подписчика) не сможет убрать рейд, пока по нему идёт рассылка.
func (h *MutexHub) publish(raidID uuid.UUID, u *Update) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	r := h.rooms[raidID]
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out, wait := r.accept(time.Now(), u)
	if wait > 0 {
		r.timer = time.AfterFunc(wait, func() { h.flush(raidID, r) })
	}
	r.broadcast(out)
}

func (h *MutexHub) flush(raidID uuid.UUID, r *mutexRoom) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.rooms[raidID] != r {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.broadcast(r.flush(time.Now()))
}
