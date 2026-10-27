package interceptors

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Limiter решает, можно ли игроку выполнить ещё один запрос.
type Limiter interface {
	// Allow возвращает false и время до следующей попытки, если лимит исчерпан.
	// Ошибка означает сбой самого ограничителя (например, недоступен Redis).
	Allow(ctx context.Context, playerID int64) (ok bool, retryAfter time.Duration, err error)
}

// UnaryRateLimit ограничивает частоту вызовов перечисленных методов по игроку.
// Должен стоять в цепочке после auth: игрока берёт из контекста.
//
// Сбой ограничителя не должен ронять игру, поэтому запрос пропускается (fail open), а сбой пишется в лог.
func UnaryRateLimit(limiter Limiter, methods ...string) grpc.UnaryServerInterceptor {
	limited := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		limited[m] = struct{}{}
	}

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if _, ok := limited[info.FullMethod]; !ok {
			return handler(ctx, req)
		}

		playerID, ok := PlayerIDFromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, PlayerIDHeader+" metadata is required")
		}

		allowed, retryAfter, err := limiter.Allow(ctx, playerID)
		if err != nil {
			slog.WarnContext(ctx, "rate limiter failed, request allowed", "err", err, "player_id", playerID)
			return handler(ctx, req)
		}
		if !allowed {
			return nil, rateLimitExceeded(retryAfter)
		}
		return handler(ctx, req)
	}
}

// rateLimitExceeded: RESOURCE_EXHAUSTED с RetryInfo, задержка округляется вверх до миллисекунды.
func rateLimitExceeded(retryAfter time.Duration) error {
	if rem := retryAfter % time.Millisecond; rem != 0 {
		retryAfter += time.Millisecond - rem
	}

	st := status.New(codes.ResourceExhausted, "rate limit exceeded")
	withInfo, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(retryAfter)})
	if err != nil {
		return st.Err()
	}
	return withInfo.Err()
}
