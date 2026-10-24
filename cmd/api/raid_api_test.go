package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/domain"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
	"github.com/andrzej80/clan-raid-go/internal/interceptors"
	"github.com/andrzej80/clan-raid-go/internal/raid"
)

// fakeRaids — in-memory реализация domain.RaidRepository с теми же правилами, что и в БД:
// клан должен существовать, активный рейд у клана не больше одного.
type fakeRaids struct {
	mu    sync.Mutex
	clans map[int64]bool
	raids map[uuid.UUID]domain.Raid
	err   error // если задана, возвращается из любого метода
}

func newFakeRaids(clans ...int64) *fakeRaids {
	f := &fakeRaids{clans: map[int64]bool{}, raids: map[uuid.UUID]domain.Raid{}}
	for _, id := range clans {
		f.clans[id] = true
	}
	return f
}

func (f *fakeRaids) CreateRaid(_ context.Context, r *domain.Raid) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	if !f.clans[r.ClanID] {
		return domain.ErrClanNotFound
	}
	for _, existing := range f.raids {
		if existing.ClanID == r.ClanID && existing.Status == domain.RaidActive {
			return domain.ErrActiveRaidExists
		}
	}
	f.raids[r.ID] = *r
	return nil
}

func (f *fakeRaids) GetRaid(_ context.Context, id uuid.UUID) (*domain.Raid, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}
	r, ok := f.raids[id]
	if !ok {
		return nil, domain.ErrRaidNotFound
	}
	return &r, nil
}

func newRaidClient(t *testing.T, repo domain.RaidRepository) raidbossV1.RaidServiceClient {
	t.Helper()

	conn := startTestServerWith(t, &config.Config{}, grpcapi.NewRaidService(raid.NewService(repo), nil, nil))
	return raidbossV1.NewRaidServiceClient(conn)
}

func validCreateRequest(clanID int64) *raidbossV1.CreateRaidRequest {
	return &raidbossV1.CreateRaidRequest{ClanId: clanID, BossName: "Dragon", HpMax: 1000, DurationSec: 3600}
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v, want %v (err: %v)", got, want, err)
	}
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, interceptors.PlayerIDHeader, "1")
}

func TestAPI_CreateAndGetRaid(t *testing.T) {
	t.Parallel()
	client := newRaidClient(t, newFakeRaids(1))
	ctx := testCtx(t)

	created, err := client.CreateRaid(ctx, validCreateRequest(1))
	if err != nil {
		t.Fatalf("CreateRaid: %v", err)
	}
	raid := created.GetRaid()
	if raid.GetClanId() != 1 || raid.GetBossName() != "Dragon" ||
		raid.GetHpMax() != 1000 || raid.GetHpLeft() != 1000 ||
		raid.GetStatus() != raidbossV1.RaidStatus_RAID_STATUS_ACTIVE {
		t.Fatalf("unexpected raid: %v", raid)
	}
	if _, err := uuid.Parse(raid.GetId()); err != nil {
		t.Fatalf("raid id %q is not a UUID", raid.GetId())
	}
	if !raid.GetEndsAt().AsTime().After(time.Now()) {
		t.Fatalf("ends_at %v must be in the future", raid.GetEndsAt().AsTime())
	}

	got, err := client.GetRaid(ctx, &raidbossV1.GetRaidRequest{RaidId: raid.GetId()})
	if err != nil {
		t.Fatalf("GetRaid: %v", err)
	}
	if got.GetRaid().GetId() != raid.GetId() || got.GetRaid().GetBossName() != "Dragon" {
		t.Fatalf("GetRaid returned %v, want %v", got.GetRaid(), raid)
	}
}

func TestAPI_CreateRaidValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*raidbossV1.CreateRaidRequest)
	}{
		{"no clan", func(r *raidbossV1.CreateRaidRequest) { r.ClanId = 0 }},
		{"negative clan", func(r *raidbossV1.CreateRaidRequest) { r.ClanId = -5 }},
		{"no boss name", func(r *raidbossV1.CreateRaidRequest) { r.BossName = "" }},
		{"zero hp", func(r *raidbossV1.CreateRaidRequest) { r.HpMax = 0 }},
		{"zero duration", func(r *raidbossV1.CreateRaidRequest) { r.DurationSec = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newRaidClient(t, newFakeRaids(1))

			req := validCreateRequest(1)
			tc.mutate(req)
			_, err := client.CreateRaid(testCtx(t), req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestAPI_CreateRaidErrors(t *testing.T) {
	t.Parallel()
	client := newRaidClient(t, newFakeRaids(1))
	ctx := testCtx(t)

	_, err := client.CreateRaid(ctx, validCreateRequest(999))
	requireCode(t, err, codes.NotFound)

	if _, err := client.CreateRaid(ctx, validCreateRequest(1)); err != nil {
		t.Fatalf("first CreateRaid: %v", err)
	}
	_, err = client.CreateRaid(ctx, validCreateRequest(1))
	requireCode(t, err, codes.AlreadyExists)
}

func TestAPI_GetRaidErrors(t *testing.T) {
	t.Parallel()
	client := newRaidClient(t, newFakeRaids(1))
	ctx := testCtx(t)

	_, err := client.GetRaid(ctx, &raidbossV1.GetRaidRequest{RaidId: "not-a-uuid"})
	requireCode(t, err, codes.InvalidArgument)

	_, err = client.GetRaid(ctx, &raidbossV1.GetRaidRequest{RaidId: uuid.NewString()})
	requireCode(t, err, codes.NotFound)
}

func TestAPI_InternalErrorIsHidden(t *testing.T) {
	t.Parallel()
	repo := newFakeRaids(1)
	repo.err = errors.New("pq: connection to 10.0.0.5 refused")
	client := newRaidClient(t, repo)

	_, err := client.GetRaid(testCtx(t), &raidbossV1.GetRaidRequest{RaidId: uuid.NewString()})
	requireCode(t, err, codes.Internal)
	if msg := status.Convert(err).Message(); msg != "internal error" {
		t.Fatalf("message = %q, internal details must not leak", msg)
	}
}

// Критерий из README: 50 одновременных CreateRaid для одного клана — 1 успех и 49 ALREADY_EXISTS.
func TestAPI_ConcurrentCreateRaidSameClan(t *testing.T) {
	t.Parallel()
	client := newRaidClient(t, newFakeRaids(1))
	ctx := testCtx(t)

	const workers = 50
	var (
		wg       sync.WaitGroup
		ok       atomic.Int32
		conflict atomic.Int32
	)
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			_, err := client.CreateRaid(ctx, validCreateRequest(1), grpc.WaitForReady(true))
			if code := status.Code(err); code == codes.OK {
				ok.Add(1)
			} else if code == codes.AlreadyExists {
				conflict.Add(1)
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	if ok.Load() != 1 || conflict.Load() != workers-1 {
		t.Fatalf("ok=%d already_exists=%d, want 1/%d", ok.Load(), conflict.Load(), workers-1)
	}
}
