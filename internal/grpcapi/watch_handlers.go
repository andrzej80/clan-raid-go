package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/watch"
)

// watchLeaderboardSize — размер топа в первом сообщении leaderboard, как и в обновлениях из Redis.
const watchLeaderboardSize = 10

// watchHub — то, что WatchRaid нужно от hub обновлений.
type watchHub interface {
	Subscribe(ctx context.Context, raidID uuid.UUID) (*watch.Subscription, error)
}

// raidGetter читает рейд для snapshot.
type raidGetter interface {
	Get(ctx context.Context, id uuid.UUID) (*domain.Raid, error)
}

// WatchHandlers — WatchRaid: snapshot из БД, текущий топ и поток обновлений из hub.
type WatchHandlers struct {
	raids raidGetter
	board leaderboardService
	hub   watchHub
}

func NewWatchHandlers(raids raidGetter, board leaderboardService, hub watchHub) *WatchHandlers {
	return &WatchHandlers{raids: raids, board: board, hub: hub}
}

// WatchRaid стримит состояние рейда. Порядок важен: сначала подписка на обновления и только
// потом чтение snapshot из БД, иначе обновления между чтением и подпиской были бы потеряны.
// Send вызывает только эта горутина, hub лишь складывает сообщения в буфер подписчика, поэтому
// клиент, который не читает, блокирует Send здесь и переполняет свой буфер, но не других клиентов.
func (h *WatchHandlers) WatchRaid(
	req *raidbossV1.WatchRaidRequest,
	stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse],
) error {
	if h == nil {
		return errNotEnabled
	}
	raidID, err := uuid.Parse(req.GetRaidId())
	if err != nil {
		return status.Error(codes.InvalidArgument, "raid_id must be a valid UUID")
	}
	ctx := stream.Context()

	sub, err := h.hub.Subscribe(ctx, raidID)
	if err != nil {
		return watchError(err)
	}
	defer sub.Close()

	raid, err := h.raids.Get(ctx, raidID)
	if err != nil {
		return err
	}
	if err := stream.Send(&raidbossV1.WatchRaidResponse{
		Update: &raidbossV1.WatchRaidResponse_Snapshot{Snapshot: raidToProto(raid)},
	}); err != nil {
		return err
	}
	if raid.Status != domain.RaidActive {
		return stream.Send(finishedFromRaid(raid))
	}
	if err := h.sendLeaderboard(ctx, stream, raidID); err != nil {
		return err
	}
	return pump(ctx, sub, stream)
}

// sendLeaderboard отправляет текущий топ сразу после snapshot: переподключившийся клиент не ждёт
// следующего удара, чтобы увидеть таблицу.
func (h *WatchHandlers) sendLeaderboard(
	ctx context.Context, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse], raidID uuid.UUID,
) error {
	entries, err := h.board.Top(ctx, raidID, watchLeaderboardSize)
	if err != nil {
		return fmt.Errorf("watch leaderboard: %w", err)
	}
	return stream.Send(&raidbossV1.WatchRaidResponse{
		Update: &raidbossV1.WatchRaidResponse_Leaderboard{Leaderboard: leaderboardToProto(entries)},
	})
}

// pump пересылает обновления подписки клиенту до finished, отключения клиента или остановки hub.
//
// Send выполняет отдельная горутина. Если клиент не читает, Send блокируется на flow control
// HTTP/2 навсегда; будь он в этой же горутине, переполнение буфера подписки (отключение
// медленного клиента) никто бы не заметил, а RPC и его ресурсы жили бы, пока клиент не вернётся.
// Теперь pump замечает отключение, завершает RPC статусом RESOURCE_EXHAUSTED, завершение RPC
// разблокирует Send, и горутина-отправитель выходит сама.
func pump(
	ctx context.Context, sub *watch.Subscription, stream grpc.ServerStreamingServer[raidbossV1.WatchRaidResponse],
) error {
	out := make(chan *watch.Update)
	sendDone := make(chan error, 1)
	go func() {
		var err error
		for update := range out {
			if err = stream.Send(update); err != nil {
				break
			}
		}
		sendDone <- err
	}()
	closeOut := sync.OnceFunc(func() { close(out) })
	defer closeOut()

	for {
		update, err := sub.Next(ctx)
		if err != nil {
			return watchError(err)
		}
		select {
		case out <- update:
		case err := <-sendDone:
			return err // клиент ушёл: Send вернул ошибку
		case <-sub.Done():
			_, err := sub.Next(ctx)
			return watchError(err)
		case <-ctx.Done():
			return ctx.Err()
		}
		if update.GetFinished() != nil {
			// Последнее сообщение: ждём, пока оно уйдёт клиенту, и завершаем RPC со статусом OK.
			closeOut()
			select {
			case err := <-sendDone:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// watchError переводит ошибки подписки в коды gRPC.
func watchError(err error) error {
	switch {
	case errors.Is(err, watch.ErrSlowConsumer):
		slog.Warn("watch client is too slow, disconnecting")
		return status.Error(codes.ResourceExhausted, "client is too slow to keep up with updates")
	case errors.Is(err, watch.ErrHubClosed):
		return status.Error(codes.Unavailable, "server is shutting down")
	default:
		return err // отмену и дедлайн контекста переводит StreamErrorMapper
	}
}

func finishedFromRaid(r *domain.Raid) *raidbossV1.WatchRaidResponse {
	finished := &raidbossV1.RaidFinished{Status: statusToProto(r.Status)}
	if r.KillerPlayerID != nil {
		finished.KillerPlayerId = *r.KillerPlayerID
	}
	return &raidbossV1.WatchRaidResponse{Update: &raidbossV1.WatchRaidResponse_Finished{Finished: finished}}
}
