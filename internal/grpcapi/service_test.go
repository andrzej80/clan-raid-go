package grpcapi_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/grpcapi"
)

// Группа хендлеров, не подключённая к RaidService, отвечает UNIMPLEMENTED на каждом своём RPC,
// а не падает на nil-получателе.
func TestRaidService_UnconfiguredGroupsAreUnimplemented(t *testing.T) {
	t.Parallel()
	svc := grpcapi.NewRaidService()
	ctx := context.Background()

	_, createErr := svc.CreateRaid(ctx, &raidbossV1.CreateRaidRequest{})
	_, getErr := svc.GetRaid(ctx, &raidbossV1.GetRaidRequest{})
	_, attackErr := svc.Attack(ctx, &raidbossV1.AttackRequest{})
	_, boardErr := svc.GetLeaderboard(ctx, &raidbossV1.GetLeaderboardRequest{})
	_, rewardsErr := svc.ListRewards(ctx, &raidbossV1.ListRewardsRequest{})
	watchErr := svc.WatchRaid(&raidbossV1.WatchRaidRequest{}, nil)

	for name, err := range map[string]error{
		"CreateRaid": createErr, "GetRaid": getErr, "Attack": attackErr,
		"GetLeaderboard": boardErr, "ListRewards": rewardsErr, "WatchRaid": watchErr,
	} {
		if got := status.Code(err); got != codes.Unimplemented {
			t.Errorf("%s: code = %v, want Unimplemented (err: %v)", name, got, err)
		}
	}
}

// Подключённая группа не включает чужие: остальные RPC по-прежнему UNIMPLEMENTED.
func TestRaidService_OnlyConfiguredGroupIsEnabled(t *testing.T) {
	t.Parallel()
	svc := grpcapi.NewRaidService(grpcapi.WithRewards(grpcapi.NewRewardsHandlers(nil)))

	if _, err := svc.GetRaid(context.Background(), &raidbossV1.GetRaidRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetRaid err = %v, want Unimplemented", err)
	}
	// ListRewards подключён: без x-player-id он отвечает не UNIMPLEMENTED, а UNAUTHENTICATED.
	if _, err := svc.ListRewards(context.Background(), &raidbossV1.ListRewardsRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ListRewards err = %v, want Unauthenticated", err)
	}
}
