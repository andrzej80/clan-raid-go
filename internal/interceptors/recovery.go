package interceptors

import (
	"context"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryRecovery превращает панику в хендлере в INTERNAL, чтобы один плохой запрос не ронял процесс.
func UnaryRecovery(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recovered(ctx, info.FullMethod, r)
		}
	}()
	return handler(ctx, req)
}

func StreamRecovery(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recovered(streamContext(ss), info.FullMethod, r)
		}
	}()
	return handler(srv, ss)
}

func recovered(ctx context.Context, method string, r any) error {
	slog.ErrorContext(ctx, "panic in handler", "method", method, "panic", r, "stack", string(debug.Stack()))
	return status.Error(codes.Internal, "internal error")
}

// streamContext возвращает контекст стрима для логов. Слой восстановления после паники не должен
// падать сам, поэтому пустой стрим даёт пустой контекст, а не панику.
func streamContext(ss grpc.ServerStream) context.Context {
	if ss == nil {
		return context.Background()
	}
	return ss.Context()
}
