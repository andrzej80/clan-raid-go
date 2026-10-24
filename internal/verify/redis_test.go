package verify

import (
	"slices"
	"testing"
)

func TestDiffLeaderboard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want map[int64]int64
		got  map[int64]int64
		out  []string
	}{
		{"equal", map[int64]int64{1: 10, 2: 20}, map[int64]int64{2: 20, 1: 10}, nil},
		{"both empty", nil, nil, nil},
		{"missing in redis", map[int64]int64{1: 10}, nil, []string{"player 1: missing in redis, postgres=10"}},
		{"extra in redis", nil, map[int64]int64{7: 5}, []string{"player 7: redis=5, missing in postgres"}},
		{
			"different score, sorted by player",
			map[int64]int64{2: 20, 1: 10},
			map[int64]int64{2: 21, 1: 11},
			[]string{"player 1: redis=11, postgres=10", "player 2: redis=21, postgres=20"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := diffLeaderboard(tt.want, tt.got); !slices.Equal(got, tt.out) {
				t.Fatalf("diff = %q, want %q", got, tt.out)
			}
		})
	}
}
