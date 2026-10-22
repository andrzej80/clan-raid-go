package interceptors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLevelFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code codes.Code
		want slog.Level
	}{
		{codes.OK, slog.LevelInfo},
		// ошибки клиента — штатная работа сервера
		{codes.InvalidArgument, slog.LevelWarn},
		{codes.NotFound, slog.LevelWarn},
		{codes.AlreadyExists, slog.LevelWarn},
		{codes.Unauthenticated, slog.LevelWarn},
		{codes.PermissionDenied, slog.LevelWarn},
		{codes.FailedPrecondition, slog.LevelWarn},
		{codes.ResourceExhausted, slog.LevelWarn},
		{codes.DeadlineExceeded, slog.LevelWarn},
		{codes.Canceled, slog.LevelWarn},
		// серверные сбои
		{codes.Internal, slog.LevelError},
		{codes.Unknown, slog.LevelError},
		{codes.DataLoss, slog.LevelError},
		{codes.Unavailable, slog.LevelError},
	}
	for _, tc := range tests {
		t.Run(tc.code.String(), func(t *testing.T) {
			t.Parallel()
			if got := levelFor(tc.code); got != tc.want {
				t.Fatalf("levelFor(%v) = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
}

// captureLogs подменяет slog.Default, поэтому тесты с ним не параллельные.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

type logRecord struct {
	Level      string  `json:"level"`
	Msg        string  `json:"msg"`
	Method     string  `json:"method"`
	Code       string  `json:"code"`
	DurationMS float64 `json:"duration_ms"`
}

func decodeLog(t *testing.T, buf *bytes.Buffer) logRecord {
	t.Helper()

	var rec logRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("log line %q is not JSON: %v", buf.String(), err)
	}
	return rec
}

func TestUnaryLogging(t *testing.T) { //nolint:paralleltest // подменяет глобальный slog.Default
	tests := []struct {
		name      string
		err       error
		wantCode  string
		wantLevel string
	}{
		{"ok", nil, "OK", "INFO"},
		{"client error", status.Error(codes.NotFound, "no raid"), "NotFound", "WARN"},
		{"server error", status.Error(codes.Internal, "boom"), "Internal", "ERROR"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			info := &grpc.UnaryServerInfo{FullMethod: "/test.Svc/Method"}

			_, err := UnaryLogging(context.Background(), nil, info, func(context.Context, any) (any, error) {
				return nil, tc.err
			})

			if !errors.Is(err, tc.err) {
				t.Fatalf("interceptor must return handler error unchanged: got %v, want %v", err, tc.err)
			}
			rec := decodeLog(t, buf)
			if rec.Msg != "grpc call" || rec.Method != "/test.Svc/Method" ||
				rec.Code != tc.wantCode || rec.Level != tc.wantLevel {
				t.Fatalf("log = %+v, want code %s level %s", rec, tc.wantCode, tc.wantLevel)
			}
			if rec.DurationMS < 0 {
				t.Fatalf("duration_ms = %v, must not be negative", rec.DurationMS)
			}
		})
	}
}

func TestStreamLogging(t *testing.T) { //nolint:paralleltest // подменяет глобальный slog.Default
	buf := captureLogs(t)
	info := &grpc.StreamServerInfo{FullMethod: "/test.Svc/Watch"}
	boom := status.Error(codes.Unavailable, "down")

	err := StreamLogging(nil, fakeStream{ctx: context.Background()}, info,
		func(any, grpc.ServerStream) error { return boom })

	if !errors.Is(err, boom) {
		t.Fatalf("interceptor must return handler error unchanged: got %v", err)
	}
	rec := decodeLog(t, buf)
	if rec.Method != "/test.Svc/Watch" || rec.Code != "Unavailable" || rec.Level != "ERROR" {
		t.Fatalf("log = %+v, want Watch/Unavailable/ERROR", rec)
	}
}
