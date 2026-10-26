// Package watch — hub живых обновлений рейдов для стрима WatchRaid.
//
// Hub принимает обновления из Feeder (в проде — Redis pub/sub, одна подписка на рейд) и
// раздаёт их локальным подписчикам. Hub никогда не вызывает stream.Send: у каждого подписчика
// свой буфер, а Send делает горутина RPC-обработчика, читая Subscription.Next. Поэтому
// медленный клиент не может задержать остальных.
//
// Интерфейс Hub реализован дважды: ActorHub (состояние принадлежит одной горутине) и
// MutexHub (состояние защищено sync.RWMutex). Поведение у них одинаковое, различается только
// способ синхронизации.
package watch

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	// DefaultBufferSize — размер буфера подписчика.
	DefaultBufferSize = 16
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

// Hub раздаёт обновления рейдов локальным подписчикам.
type Hub interface {
	// Subscribe регистрирует подписчика. К моменту возврата подписка на источник уже
	// запущена, поэтому вызывающий читает snapshot из БД ПОСЛЕ Subscribe: обновления,
	// пришедшие между чтением и регистрацией, иначе были бы потеряны.
	Subscribe(ctx context.Context, raidID uuid.UUID) (*Subscription, error)
	// Close останавливает hub: все подписчики получают ErrHubClosed. Идемпотентен.
	Close()
}

// Feeder — источник обновлений рейдов (Redis pub/sub).
//
// Start не блокируется и не вызывает sink до своего возврата. Возвращаемый stop тоже не
// блокируется и не ждёт завершения уже начатых вызовов sink: hub вызывает его под своей
// блокировкой, а sink в это время может ждать её же. sink может вызываться только из одной
// горутины на рейд, порядок сообщений сохраняется.
type Feeder interface {
	Start(raidID uuid.UUID, sink func(*Update)) (stop func())
}

// Config настраивает hub.
type Config struct {
	// BufferSize — размер буфера подписчика; 0 — DefaultBufferSize.
	BufferSize int
	// Interval — минимальный интервал между обновлениями рейда; 0 — DefaultInterval,
	// отрицательное значение отключает троттлинг.
	Interval time.Duration
}

func (c Config) withDefaults() Config {
	if c.BufferSize <= 0 {
		c.BufferSize = DefaultBufferSize
	}
	if c.Interval == 0 {
		c.Interval = DefaultInterval
	}
	return c
}
