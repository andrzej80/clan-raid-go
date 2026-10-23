// Package ratelimit содержит ограничители частоты запросов.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const minSweepSize = 1024

// Memory — token bucket на игрока в памяти процесса. Лимит действует на один экземпляр api,
// а не на весь кластер: общий лимит на Redis появится на этапе 4 за тем же интерфейсом.
type Memory struct {
	rate  rate.Limit
	burst int
	now   func() time.Time

	mu      sync.Mutex
	entries map[int64]*entry
	sweepAt int
}

type entry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type Option func(*Memory)

// WithClock подменяет источник времени (для тестов).
func WithClock(now func() time.Time) Option {
	return func(m *Memory) { m.now = now }
}

// NewMemory: perSecond запросов в секунду, всплеск до burst.
func NewMemory(perSecond float64, burst int, opts ...Option) *Memory {
	m := &Memory{
		rate:    rate.Limit(perSecond),
		burst:   burst,
		now:     time.Now,
		entries: make(map[int64]*entry),
		sweepAt: minSweepSize,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Allow расходует один токен игрока. Если токенов нет, возвращает, через сколько появится следующий.
func (m *Memory) Allow(_ context.Context, playerID int64) (bool, time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	e := m.entry(playerID, now)

	reservation := e.limiter.ReserveN(now, 1)
	if delay := reservation.DelayFrom(now); !reservation.OK() || delay > 0 {
		reservation.CancelAt(now) // отказ не должен списывать токен
		return false, delay, nil
	}
	e.lastSeen = now
	return true, 0, nil
}

func (m *Memory) entry(playerID int64, now time.Time) *entry {
	if e, ok := m.entries[playerID]; ok {
		return e
	}
	e := &entry{limiter: rate.NewLimiter(m.rate, m.burst), lastSeen: now}
	m.entries[playerID] = e

	// Игрок, не бившийся дольше времени полного пополнения, неотличим от нового: запись можно удалить.
	if len(m.entries) > m.sweepAt {
		refill := time.Duration(float64(m.burst) / float64(m.rate) * float64(time.Second))
		for id, old := range m.entries {
			if id != playerID && now.Sub(old.lastSeen) >= refill {
				delete(m.entries, id)
			}
		}
		m.sweepAt = max(minSweepSize, 2*len(m.entries))
	}
	return e
}
