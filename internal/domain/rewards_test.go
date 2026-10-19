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
				{100, GeneralKind, 800},
				{130, GeneralKind, 400},
				{100, TopKind, 500},
				{130, TopKind, 300},
				{130, KillerKind, 250},
			},
		},
		{
			name: "killer not in list", damageByUser: map[int]int{100: 70, 130: 30}, killerId: 10,
			resultReward: []Reward{
				{100, GeneralKind, 800},
				{130, GeneralKind, 400},
				{100, TopKind, 500},
				{130, TopKind, 300},
			},
		},
		{
			name: "empty damage", damageByUser: map[int]int{}, killerId: 1,
			resultReward: []Reward{},
		},
		{
			name: "single player is also killer", damageByUser: map[int]int{7: 50}, killerId: 7,
			resultReward: []Reward{
				{7, GeneralKind, 1100},
				{7, TopKind, 500},
				{7, KillerKind, 250},
			},
		},
		{
			// 1000*1/3 = 333.33 -> floor 333; при равном уроне места по возрастанию id
			name: "participation is floored", damageByUser: map[int]int{1: 1, 2: 1, 3: 1}, killerId: 2,
			resultReward: []Reward{
				{1, GeneralKind, 433},
				{2, GeneralKind, 433},
				{3, GeneralKind, 433},
				{1, TopKind, 500},
				{2, TopKind, 300},
				{3, TopKind, 200},
				{2, KillerKind, 250},
			},
		},
		{
			name: "top3 only for three players", damageByUser: map[int]int{1: 40, 2: 30, 3: 20, 4: 10}, killerId: 4,
			resultReward: []Reward{
				{1, GeneralKind, 500},
				{2, GeneralKind, 400},
				{3, GeneralKind, 300},
				{4, GeneralKind, 200},
				{1, TopKind, 500},
				{2, TopKind, 300},
				{3, TopKind, 200},
				{4, KillerKind, 250},
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
				{100, TopKind, 500},
				{70, TopKind, 300},
				{80, TopKind, 200},
			},
		},
		{
			name: "empty damage", damageByUser: map[int]int{},
			resultReward: []Reward{},
		},
		{
			name: "same points", damageByUser: map[int]int{80: 30, 100: 70, 130: 30, 70: 30},
			resultReward: []Reward{
				{100, TopKind, 500},
				{70, TopKind, 300},
				{80, TopKind, 200},
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
