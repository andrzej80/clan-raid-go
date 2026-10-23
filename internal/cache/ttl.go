// Package cache содержит in-process кэш с TTL и схлопыванием одновременных загрузок.
package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// loadTimeout ограничивает загрузку, которая не зависит от отмены отдельных вызывающих.
	loadTimeout = 5 * time.Second
	// minSweepSize — размер карты, до которого протухшие записи не вычищаются.
	minSweepSize = 1024
)

// Loader загружает значение по ключу из источника правды.
type Loader[K comparable, V any] func(ctx context.Context, key K) (V, error)

type Option func(*options)

type options struct {
	now func() time.Time
}

// WithClock подменяет источник времени (для тестов).
func WithClock(now func() time.Time) Option {
	return func(o *options) { o.now = now }
}

type entry[V any] struct {
	value   V
	expires time.Time
}

// TTL хранит значения ограниченное время. Промах кэша идёт через singleflight:
// N одновременных запросов по одному ключу дают одну загрузку, а не N.
//
// Значения общие для всех вызывающих, поэтому их нельзя изменять. Ошибки не кэшируются.
type TTL[K comparable, V any] struct {
	ttl  time.Duration
	load Loader[K, V]
	now  func() time.Time

	group singleflight.Group

	mu      sync.RWMutex
	items   map[K]entry[V]
	sweepAt int
}

func NewTTL[K comparable, V any](ttl time.Duration, load Loader[K, V], opts ...Option) *TTL[K, V] {
	o := options{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	return &TTL[K, V]{
		ttl:     ttl,
		load:    load,
		now:     o.now,
		items:   make(map[K]entry[V]),
		sweepAt: minSweepSize,
	}
}

// Get возвращает значение из кэша или загружает его.
//
// Загрузка запускается в контексте без отмены вызывающего (но с его значениями, например трейсом)
// и с собственным таймаутом: если инициатор ушёл по дедлайну, остальные ожидающие
// не получают чужую ошибку. Каждый вызывающий при этом выходит по своему ctx.
func (c *TTL[K, V]) Get(ctx context.Context, key K) (V, error) {
	if v, ok := c.fresh(key); ok {
		return v, nil
	}

	ch := c.group.DoChan(fmt.Sprint(key), func() (any, error) {
		// Между промахом и началом загрузки её мог закончить другой вызов.
		if v, ok := c.fresh(key); ok {
			return v, nil
		}

		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), loadTimeout)
		defer cancel()

		v, err := c.load(loadCtx, key)
		if err != nil {
			return nil, err
		}
		c.store(key, v)
		return v, nil
	})

	select {
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			var zero V
			return zero, res.Err
		}
		return res.Val.(V), nil //nolint:forcetypeassert // в группу кладётся только V
	}
}

func (c *TTL[K, V]) fresh(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.items[key]
	if !ok || !c.now().Before(e.expires) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *TTL[K, V]) store(key K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.items[key] = entry[V]{value: v, expires: now.Add(c.ttl)}

	// Без этого карта росла бы вечно: протухшие записи сами не удаляются.
	if len(c.items) > c.sweepAt {
		for k, e := range c.items {
			if !now.Before(e.expires) {
				delete(c.items, k)
			}
		}
		c.sweepAt = max(minSweepSize, 2*len(c.items))
	}
}
