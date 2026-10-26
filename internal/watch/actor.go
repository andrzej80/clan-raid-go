package watch

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

const actorInboxSize = 1024

// ActorHub — реализация Hub на одной горутине-владельце состояния. Карта рейдов и подписчиков
// доступна только ей, остальные горутины общаются с ней сообщениями через inbox. Мьютексов
// для состояния нет.
type ActorHub struct {
	cfg    Config
	feeder Feeder

	inbox     chan any
	quit      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

var _ Hub = (*ActorHub)(nil)

type (
	subscribeMsg struct {
		raidID uuid.UUID
		sub    *Subscription
		ack    chan struct{}
	}
	unsubscribeMsg struct {
		raidID uuid.UUID
		sub    *Subscription
	}
	updateMsg struct {
		raidID uuid.UUID
		update *Update
	}
	flushMsg struct {
		raidID uuid.UUID
		room   *room
	}
)

// NewActorHub запускает горутину hub'а; остановить её нужно через Close.
func NewActorHub(feeder Feeder, cfg Config) *ActorHub {
	h := &ActorHub{
		cfg:     cfg.withDefaults(),
		feeder:  feeder,
		inbox:   make(chan any, actorInboxSize),
		quit:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go h.loop()
	return h
}

func (h *ActorHub) Subscribe(ctx context.Context, raidID uuid.UUID) (*Subscription, error) {
	sub := newSubscription(h.cfg.BufferSize)
	sub.release = func() { h.post(unsubscribeMsg{raidID: raidID, sub: sub}) }
	ack := make(chan struct{})

	select {
	case h.inbox <- subscribeMsg{raidID: raidID, sub: sub, ack: ack}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.stopped:
		return nil, ErrHubClosed
	}
	select {
	case <-ack:
		return sub, nil
	case <-ctx.Done():
		sub.Close() // сообщение уже в очереди: unsubscribe встанет за ним
		return nil, ctx.Err()
	case <-h.stopped:
		return nil, ErrHubClosed
	}
}

func (h *ActorHub) Close() {
	h.closeOnce.Do(func() { close(h.quit) })
	<-h.stopped
}

// post отправляет сообщение актору и не зависает, если он уже остановился.
func (h *ActorHub) post(m any) {
	select {
	case h.inbox <- m:
	case <-h.stopped:
	}
}

func (h *ActorHub) loop() {
	defer close(h.stopped)
	rooms := make(map[uuid.UUID]*room)
	defer func() {
		for _, r := range rooms {
			r.shutdown(ErrHubClosed)
		}
	}()

	for {
		select {
		case <-h.quit:
			return
		case m := <-h.inbox:
			h.handle(rooms, m)
		}
	}
}

func (h *ActorHub) handle(rooms map[uuid.UUID]*room, m any) {
	switch m := m.(type) {
	case subscribeMsg:
		r := rooms[m.raidID]
		if r == nil {
			r = newRoom(h.cfg.Interval)
			r.stopFeed = h.feeder.Start(m.raidID, func(u *Update) {
				h.post(updateMsg{raidID: m.raidID, update: u})
			})
			rooms[m.raidID] = r
		}
		r.subs[m.sub] = struct{}{}
		close(m.ack)

	case unsubscribeMsg:
		r := rooms[m.raidID]
		if r == nil {
			return
		}
		delete(r.subs, m.sub)
		if len(r.subs) == 0 {
			r.shutdown(nil)
			delete(rooms, m.raidID)
		}

	case updateMsg:
		r := rooms[m.raidID]
		if r == nil {
			return // запоздавшее сообщение уже остановленного источника
		}
		out, wait := r.accept(time.Now(), m.update)
		if wait > 0 {
			r.timer = time.AfterFunc(wait, func() { h.post(flushMsg{raidID: m.raidID, room: r}) })
		}
		r.broadcast(out)

	case flushMsg:
		if rooms[m.raidID] != m.room {
			return
		}
		m.room.broadcast(m.room.flush(time.Now()))
	}
}
