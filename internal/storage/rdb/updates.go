package rdb

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	updatesBackoffMin = 100 * time.Millisecond
	updatesBackoffMax = 5 * time.Second
)

// Updates — источник обновлений для watch.Hub: по горутине и подписке Redis pub/sub на рейд.
// Реализует watch.Feeder.
type Updates struct {
	rc  redis.UniversalClient
	wg  sync.WaitGroup
	ctx context.Context //nolint:containedctx // корневой контекст всех подписок, отменяется в Close
	end context.CancelFunc
}

func NewUpdates(rc redis.UniversalClient) *Updates {
	ctx, cancel := context.WithCancel(context.Background())
	return &Updates{rc: rc, ctx: ctx, end: cancel}
}

// Start запускает чтение канала рейда и сразу возвращается. ready закрывается после
// подтверждения подписки Redis'ом. Пока Redis недоступен, горутина повторяет попытки с
// backoff; после того как подписка установлена, переподключение делает сам go-redis.
// stop не ждёт горутину: её завершение ждёт Close.
func (u *Updates) Start(raidID uuid.UUID, sink func(*raidbossV1.WatchRaidResponse)) (stop func(), ready <-chan struct{}) {
	ctx, cancel := context.WithCancel(u.ctx)
	readyCh := make(chan struct{})
	u.wg.Go(func() { u.run(ctx, raidID, sink, readyCh) })
	return cancel, readyCh
}

// Close останавливает все подписки и ждёт завершения их горутин.
func (u *Updates) Close() {
	u.end()
	u.wg.Wait()
}

func (u *Updates) run(
	ctx context.Context, raidID uuid.UUID, sink func(*raidbossV1.WatchRaidResponse), ready chan struct{},
) {
	backoff := updatesBackoffMin
	readySignaled := false
	for ctx.Err() == nil {
		ps := u.rc.Subscribe(ctx, UpdatesChannel(raidID))
		if _, err := ps.Receive(ctx); err != nil { // подтверждение подписки
			closePubSub(ps)
			if ctx.Err() != nil {
				return
			}
			slog.Warn("redis updates subscribe failed", "raid_id", raidID, "err", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, updatesBackoffMax)
			continue
		}
		backoff = updatesBackoffMin
		if !readySignaled {
			close(ready)
			readySignaled = true
		}

		// Закрытие ps при отмене останавливает Channel(): без этого range завис бы навсегда.
		stopClose := context.AfterFunc(ctx, func() { closePubSub(ps) })
		for msg := range ps.Channel() {
			var update raidbossV1.WatchRaidResponse
			if err := proto.Unmarshal([]byte(msg.Payload), &update); err != nil {
				slog.Warn("redis updates: bad payload", "raid_id", raidID, "err", err)
				continue
			}
			sink(&update)
		}
		stopClose()
		closePubSub(ps)
	}
}

// sleep ждёт d или отмены ctx; false — контекст отменён.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// closePubSub закрывает подписку; ошибку закрытия обработать нечем.
func closePubSub(ps *redis.PubSub) {
	_ = ps.Close() //nolint:gosec // закрытие при выходе, ошибку обработать нечем
}
