package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryRecovery(t *testing.T) {
	t.Parallel()

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Panic"}
	_, err := UnaryRecovery(context.Background(), nil, info, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v, want Internal", got)
	}
	if msg := status.Convert(err).Message(); msg != "internal error" {
		t.Fatalf("message = %q, panic details must not leak", msg)
	}
}

func TestUnaryRecovery_PassesThrough(t *testing.T) {
	t.Parallel()

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Ok"}
	resp, err := UnaryRecovery(context.Background(), nil, info, func(context.Context, any) (any, error) {
		return "ok", nil
	})
	if err != nil || resp != "ok" {
		t.Fatalf("resp=%v err=%v, want ok/nil", resp, err)
	}
}

func TestStreamRecovery(t *testing.T) {
	t.Parallel()

	info := &grpc.StreamServerInfo{FullMethod: "/test/Panic"}
	err := StreamRecovery(nil, nil, info, func(any, grpc.ServerStream) error { panic("boom") })
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v, want Internal", got)
	}
}
