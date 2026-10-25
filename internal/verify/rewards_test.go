package verify

import (
	"strings"
	"testing"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

func TestDiffRewards(t *testing.T) {
	t.Parallel()

	want := []domain.Reward{
		{PlayerID: 1, Kind: domain.GeneralKind, Amount: 850},
		{PlayerID: 1, Kind: domain.TopKind, Amount: 500},
	}
	have := func() map[rewardKey]int {
		return map[rewardKey]int{
			{1, domain.GeneralKind}: 850,
			{1, domain.TopKind}:     500,
		}
	}

	if d := diffRewards(want, have()); d != "" {
		t.Fatalf("matching rewards reported as different: %q", d)
	}

	wrong := have()
	wrong[rewardKey{1, domain.TopKind}] = 300
	if d := diffRewards(want, wrong); !strings.Contains(d, "top3 for player 1 is 300, want 500") {
		t.Fatalf("diff = %q", d)
	}

	missing := have()
	delete(missing, rewardKey{1, domain.GeneralKind})
	if d := diffRewards(want, missing); !strings.Contains(d, "missing participation for player 1") {
		t.Fatalf("diff = %q", d)
	}

	extra := have()
	extra[rewardKey{2, domain.KillerKind}] = 250
	if d := diffRewards(want, extra); !strings.Contains(d, "unexpected killing_blow for player 2") {
		t.Fatalf("diff = %q", d)
	}
}

func TestDiffRewardsTruncatesLongReports(t *testing.T) {
	t.Parallel()

	var want []domain.Reward
	for i := 1; i <= 10; i++ {
		want = append(want, domain.Reward{PlayerID: i, Kind: domain.GeneralKind, Amount: 100})
	}
	d := diffRewards(want, map[rewardKey]int{})
	if !strings.Contains(d, "and 7 more") || strings.Count(d, "missing") != 3 {
		t.Fatalf("diff = %q, want 3 examples and a summary", d)
	}
}
