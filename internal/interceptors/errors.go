package interceptors

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// UnaryErrorMapper превращает доменные ошибки из хендлеров в коды gRPC.
// Ошибки, которые уже являются gRPC-статусами, проходят без изменений.
func UnaryErrorMapper(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := handler(ctx, req)
	return resp, toStatus(ctx, err)
}

func StreamErrorMapper(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return toStatus(streamContext(ss), handler(srv, ss))
}

func toStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}

	switch {
	case errors.Is(err, domain.ErrRaidNotFound),
		errors.Is(err, domain.ErrClanNotFound),
		errors.Is(err, domain.ErrPlayerNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrWrongClan):
		return status.Error(codes.PermissionDenied, domain.ErrWrongClan.Error())
	case errors.Is(err, domain.ErrRaidNotActive):
		return status.Error(codes.FailedPrecondition, domain.ErrRaidNotActive.Error())
	case errors.Is(err, domain.ErrQueueUnavailable):
		// Текст фиксированный: детали брокера клиенту не нужны, они в логе сервиса.
		return status.Error(codes.Unavailable, domain.ErrQueueUnavailable.Error())
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrActiveRaidExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		slog.ErrorContext(ctx, "internal error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}
