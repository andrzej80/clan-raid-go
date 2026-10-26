package watch

import (
	"time"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// room — состояние одного рейда внутри hub: подписчики, подписка на источник и троттлинг.
// Сам по себе не потокобезопасен: Hub синхронизирует доступ.
//
// Троттлинг: первое обновление после паузы уходит сразу, дальше не чаще одного раза за
// interval. Накопленные за интервал hp_left и leaderboard схлопываются до последних значений.
type room struct {
	subs     map[*Subscription]struct{}
	stopFeed func()
	timer    *time.Timer // таймер отложенной отправки; создаёт владелец по результату accept
	interval time.Duration

	last     time.Time
	armed    bool // ждём срабатывания таймера
	finished bool
	hp       *Update
	lb       *Update
}

func newRoom(interval time.Duration) *room {
	return &room{subs: make(map[*Subscription]struct{}), interval: interval}
}

// accept принимает обновление из источника. out нужно разослать сейчас; wait > 0 означает,
// что владелец обязан вызвать flush через wait.
func (r *room) accept(now time.Time, u *Update) (out []*Update, wait time.Duration) {
	if r.finished {
		return nil, 0
	}
	switch u.GetUpdate().(type) {
	case *raidbossV1.WatchRaidResponse_Finished:
		// Конец рейда не троттлится, а накопленные обновления уже не нужны.
		r.finished = true
		r.hp, r.lb, r.armed = nil, nil, false
		r.stopTimer()
		return []*Update{u}, 0
	case *raidbossV1.WatchRaidResponse_HpLeft:
		r.hp = u
	case *raidbossV1.WatchRaidResponse_Leaderboard:
		r.lb = u
	default:
		return []*Update{u}, 0
	}

	if r.armed {
		return nil, 0
	}
	since := now.Sub(r.last)
	if r.interval <= 0 || since >= r.interval {
		r.last = now
		return r.drain(), 0
	}
	r.armed = true
	return nil, r.interval - since
}

// flush вызывается по таймеру и возвращает накопленное за интервал.
func (r *room) flush(now time.Time) []*Update {
	r.armed = false
	out := r.drain()
	if len(out) > 0 {
		r.last = now
	}
	return out
}

func (r *room) drain() []*Update {
	var out []*Update
	if r.hp != nil {
		out = append(out, r.hp)
	}
	if r.lb != nil {
		out = append(out, r.lb)
	}
	r.hp, r.lb = nil, nil
	return out
}

func (r *room) broadcast(updates []*Update) {
	for _, u := range updates {
		for s := range r.subs {
			s.offer(u)
		}
	}
}

func (r *room) stopTimer() {
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
}

// shutdown останавливает источник и таймер. Если err не nil, подписчики закрываются с ним.
func (r *room) shutdown(err error) {
	if r.stopFeed != nil {
		r.stopFeed()
		r.stopFeed = nil
	}
	r.stopTimer()
	if err != nil {
		for s := range r.subs {
			s.kill(err)
		}
	}
	clear(r.subs)
}
