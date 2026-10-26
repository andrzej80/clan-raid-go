// Package watch — hub живых обновлений рейдов для стрима WatchRaid.
//
// Hub принимает обновления из Feeder (в проде — Redis pub/sub, одна подписка на рейд) и
// раздаёт их локальным подписчикам. Hub никогда не вызывает stream.Send: у каждого подписчика
// свой буфер, а Send делает горутина RPC-обработчика, читая Subscription.Next. Поэтому
// медленный клиент не может задержать остальных.
//
// Состояние защищено sync.RWMutex. Вариант с горутиной-владельцем состояния (actor) был
// реализован и измерен, но проиграл на нагрузке с многими рейдами и при штормах
// переподключений; сравнение — в docs/pr-stage-6.md.
package watch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	// DefaultBufferSize — размер буфера подписчика.
	DefaultBufferSize = 16
	// DefaultReadyTimeout — сколько Subscribe ждёт готовности источника.
	DefaultReadyTimeout = 2 * time.Second
	// DefaultInterval — минимальный интервал между обновлениями рейда (не чаще 4 раз в секунду).
	DefaultInterval = 250 * time.Millisecond
)

var (
	// ErrSlowConsumer: буфер подписчика переполнился повторно, клиент отключён.
	ErrSlowConsumer = errors.New("watch: subscriber is too slow")
	// ErrClosed: подписчик сам закрыл подписку.
	ErrClosed = errors.New("watch: subscription closed")
	// ErrHubClosed: hub остановлен.
	ErrHubClosed = errors.New("watch: hub closed")
)

// Update — сообщение стрима.
type Update = raidbossV1.WatchRaidResponse

// Feeder — источник обновлений рейдов (Redis pub/sub).
//
// Start не блокируется и не вызывает sink до своего возврата. ready закрывается, когда
// подписка на источник действительно активна (для Redis — получено подтверждение SUBSCRIBE):
// до этого момента обновления могут теряться. Возвращаемый stop тоже не блокируется и не ждёт
// завершения уже начатых вызовов sink: hub вызывает его под своей блокировкой, а sink в это
// время может ждать её же. sink может вызываться только из одной горутины на рейд, порядок
// сообщений сохраняется.
type Feeder interface {
	Start(raidID uuid.UUID, sink func(*Update)) (stop func(), ready <-chan struct{})
}

// Config настраивает hub.
type Config struct {
	// BufferSize — размер буфера подписчика; 0 — DefaultBufferSize.
	BufferSize int
	// Interval — минимальный интервал между обновлениями рейда; 0 — DefaultInterval,
	// отрицательное значение отключает троттлинг.
	Interval time.Duration
	// ReadyTimeout — сколько Subscribe ждёт активации подписки на источник; 0 —
	// DefaultReadyTimeout. По истечении подписчик всё равно получает подписку: потерянное
	// обновление исправит следующее (hp_left абсолютный), а состояние на момент подключения
	// клиент получает из snapshot.
	ReadyTimeout time.Duration
	// MeterProvider необязателен: по умолчанию используется глобальный (см. telemetry.Setup).
	MeterProvider metric.MeterProvider
}

func (c Config) withDefaults() Config {
	if c.BufferSize <= 0 {
		c.BufferSize = DefaultBufferSize
	}
	if c.Interval == 0 {
		c.Interval = DefaultInterval
	}
	if c.ReadyTimeout <= 0 {
		c.ReadyTimeout = DefaultReadyTimeout
	}
	return c
}

// Hub раздаёт обновления рейдов локальным подписчикам.
//
// mu защищает карту рейдов и множества подписчиков: раздача обновления идёт под RLock, то
// есть рейды обрабатываются параллельно, а подписка и отписка берут Lock. hubRoom.mu
// сериализует accept/flush/broadcast одного рейда, чтобы порядок обновлений не нарушался.
// Блокировки берутся всегда в порядке mu → hubRoom.mu → Subscription.mu.
type Hub struct {
	cfg    Config
	feeder Feeder

	mu     sync.RWMutex
	rooms  map[uuid.UUID]*hubRoom
	closed bool

	metrics *hubMetrics
}

type hubRoom struct {
	*room
	mu sync.Mutex
}

func New(feeder Feeder, cfg Config) *Hub {
	h := &Hub{
		cfg:    cfg.withDefaults(),
		feeder: feeder,
		rooms:  make(map[uuid.UUID]*hubRoom),
	}
	metrics, err := newHubMetrics(cfg.MeterProvider, h)
	if err != nil {
		// Метрики не должны мешать раздаче обновлений.
		slog.Warn("watch metrics disabled", "err", err)
		metrics = noopHubMetrics()
	}
	h.metrics = metrics
	return h
}

// counts возвращает число подписчиков и рейдов с подписчиками (для метрик).
func (h *Hub) counts() (subscribers, rooms int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, r := range h.rooms {
		subscribers += int64(len(r.subs))
	}
	return subscribers, int64(len(h.rooms))
}

// Subscribe регистрирует подписчика. К моменту возврата подписка на источник уже активна
// (или истёк ReadyTimeout), поэтому вызывающий читает snapshot из БД ПОСЛЕ Subscribe:
// обновления, пришедшие между чтением и регистрацией, иначе были бы потеряны.
func (h *Hub) Subscribe(ctx context.Context, raidID uuid.UUID) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sub := newSubscription(h.cfg.BufferSize)
	sub.release = func() { h.unsubscribe(raidID, sub) }
	// Вызывается из горутины источника при раздаче обновления: контекста запроса там нет.
	sub.onSlow = func() { h.metrics.slowDisconnects.Add(context.Background(), 1) } //nolint:contextcheck // колбэк из горутины источника

	ready, err := h.register(raidID, sub)
	if err != nil {
		return nil, err
	}
	if err := h.awaitReady(ctx, raidID, ready); err != nil {
		sub.Close()
		return nil, err
	}
	return sub, nil
}

func (h *Hub) register(raidID uuid.UUID, sub *Subscription) (<-chan struct{}, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHubClosed
	}
	r := h.rooms[raidID]
	if r == nil {
		r = &hubRoom{room: newRoom(h.cfg.Interval)}
		r.stopFeed, r.ready = h.feeder.Start(raidID, func(u *Update) { h.publish(raidID, u) })
		h.rooms[raidID] = r
	}
	r.subs[sub] = struct{}{}
	return r.ready, nil
}

func (h *Hub) awaitReady(ctx context.Context, raidID uuid.UUID, ready <-chan struct{}) error {
	select {
	case <-ready:
		return nil
	default:
	}
	timer := time.NewTimer(h.cfg.ReadyTimeout)
	defer timer.Stop()
	select {
	case <-ready:
	case <-timer.C:
		slog.Warn("watch source is not ready, subscribing anyway", "raid_id", raidID)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Close останавливает hub: все подписчики получают ErrHubClosed. Идемпотентен.
func (h *Hub) Close() {
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

func (h *Hub) unsubscribe(raidID uuid.UUID, sub *Subscription) {
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
func (h *Hub) publish(raidID uuid.UUID, u *Update) {
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

func (h *Hub) flush(raidID uuid.UUID, r *hubRoom) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.rooms[raidID] != r {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.broadcast(r.flush(time.Now()))
}
