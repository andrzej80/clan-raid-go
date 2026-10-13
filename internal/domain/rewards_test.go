package domain

import (
	"cmp"
	"slices"
	"testing"
)

func TestCalcRewards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		damageByUser map[int]int
		killerId     int
		resultReward []Reward
	}{
		{
			name: "2 players", damageByUser: map[int]int{100: 70, 130: 30}, killerId: 130,
			resultReward: []Reward{
				{100, general, 800},
				{130, general, 400},
				{100, top, 500},
				{130, top, 300},
				{130, killer, 250},
			},
		},
		{
			name: "killer not in list", damageByUser: map[int]int{100: 70, 130: 30}, killerId: 10,
			resultReward: []Reward{
				{100, general, 800},
				{130, general, 400},
				{100, top, 500},
				{130, top, 300},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CalcRewards(tc.damageByUser, tc.killerId)
			want := slices.Clone(tc.resultReward)
			sortRewards(got)
			sortRewards(want)
			if !slices.Equal(got, want) {
				t.Fatalf("CalcRewards() got = %v, want %v", got, want)
			}
		})
	}
}

func TestAddTopRewards(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		damageByUser map[int]int
		resultReward []Reward
	}{
		{
			name: "same points", damageByUser: map[int]int{100: 70, 130: 30, 80: 30, 70: 30},
			resultReward: []Reward{
				{100, top, 500},
				{70, top, 300},
				{80, top, 200},
			},
		},
		{
			name: "empty damage", damageByUser: map[int]int{},
			resultReward: []Reward{},
		},
		{
			name: "same points", damageByUser: map[int]int{80: 30, 100: 70, 130: 30, 70: 30},
			resultReward: []Reward{
				{100, top, 500},
				{70, top, 300},
				{80, top, 200},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rewards := make([]Reward, 0, 3)
			addTopRewards(&rewards, tc.damageByUser)
			want := slices.Clone(tc.resultReward)
			sortRewards(rewards)
			sortRewards(want)
			if !slices.Equal(rewards, want) {
				t.Fatalf("CalcRewards() got = %v, want %v", rewards, want)
			}
		})
	}
}

func sortRewards(r []Reward) {
	slices.SortFunc(r, func(a, b Reward) int {
		return cmp.Or(
			cmp.Compare(a.PlayerID, b.PlayerID),
			cmp.Compare(a.Kind, b.Kind),
		)
	})
}
