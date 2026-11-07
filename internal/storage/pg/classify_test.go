package pg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

func TestClassifyError(t *testing.T) {
	t.Parallel()

	pgErr := func(code string) error {
		return fmt.Errorf("raid 1: insert hits: %w", &pgconn.PgError{Code: code})
	}
	tests := []struct {
		name string
		err  error
		want domain.ErrorClass
	}{
		{"deadline", fmt.Errorf("lock raid: %w", context.DeadlineExceeded), domain.ErrorTransient},
		{"canceled", context.Canceled, domain.ErrorTransient},
		{"serialization failure", pgErr("40001"), domain.ErrorTransient},
		{"deadlock", pgErr("40P01"), domain.ErrorTransient},
		{"connection failure", pgErr("08006"), domain.ErrorTransient},
		{"too many connections", pgErr("53300"), domain.ErrorTransient},
		{"lock not available", pgErr("55P03"), domain.ErrorTransient},
		{"admin shutdown", pgErr("57P01"), domain.ErrorTransient},
		{"query canceled", pgErr("57014"), domain.ErrorTransient},
		{"io error", pgErr("58030"), domain.ErrorTransient},
		{"numeric out of range", pgErr("22003"), domain.ErrorPermanent},
		{"invalid text", pgErr("22P02"), domain.ErrorPermanent},
		{"check violation", pgErr("23514"), domain.ErrorPermanent},
		{"unique violation", pgErr("23505"), domain.ErrorPermanent},
		{"undefined column (bad release)", pgErr("42703"), domain.ErrorUnknown},
		{"undefined table (missing migration)", pgErr("42P01"), domain.ErrorUnknown},
		{"internal error", pgErr("XX000"), domain.ErrorUnknown},
		{"short code", pgErr("4"), domain.ErrorUnknown},
		{"network", fmt.Errorf("query: %w", &net.OpError{Op: "read", Err: errors.New("connection reset")}), domain.ErrorTransient},
		{"connect", &pgconn.ConnectError{}, domain.ErrorTransient},
		{"unexpected eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), domain.ErrorTransient},
		{"plain error", errors.New("boom"), domain.ErrorUnknown},
		{"nil", nil, domain.ErrorUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyError(tc.err); got != tc.want {
				t.Fatalf("ClassifyError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
