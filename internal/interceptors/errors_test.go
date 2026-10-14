package interceptors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

func TestToStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"nil", nil, codes.OK},
		{"not found", domain.ErrRaidNotFound, codes.NotFound},
		{"wrapped not found", fmt.Errorf("get: %w", domain.ErrRaidNotFound), codes.NotFound},
		{"clan not found", domain.ErrClanNotFound, codes.NotFound},
		{"already exists", domain.ErrActiveRaidExists, codes.AlreadyExists},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"canceled", context.Canceled, codes.Canceled},
		{"existing status kept", status.Error(codes.InvalidArgument, "bad"), codes.InvalidArgument},
		{"unknown", errors.New("boom"), codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := status.Code(toStatus(tc.err)); got != tc.want {
				t.Fatalf("code = %v, want %v", got, tc.want)
			}
		})
	}
}
