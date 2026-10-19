package interceptors

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryLogging пишет одну строку на вызов: метод, код ответа, длительность.
func UnaryLogging(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	logCall(ctx, info.FullMethod, err, time.Since(start))
	return resp, err
}

func StreamLogging(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	err := handler(srv, ss)
	logCall(ss.Context(), info.FullMethod, err, time.Since(start))
	return err
}

func logCall(ctx context.Context, method string, err error, d time.Duration) {
	code := status.Code(err)
	slog.Log(ctx, levelFor(code), "grpc call",
		"method", method,
		"code", code.String(),
		"duration_ms", float64(d.Microseconds())/1000,
	)
}

// levelFor: ошибки клиента — штатная работа сервера, ERROR только для серверных сбоев.
func levelFor(code codes.Code) slog.Level {
	if code == codes.OK {
		return slog.LevelInfo
	}
	if _, isServerFault := serverFaultCodes[code]; isServerFault {
		return slog.LevelError
	}
	return slog.LevelWarn
}

var serverFaultCodes = map[codes.Code]struct{}{
	codes.Internal:    {},
	codes.Unknown:     {},
	codes.DataLoss:    {},
	codes.Unavailable: {},
}
