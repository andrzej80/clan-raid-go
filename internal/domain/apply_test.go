package domain

import (
	"slices"
	"testing"

	"github.com/google/uuid"
)

func hitsOf(damages ...int) []Hit {
	out := make([]Hit, len(damages))
	for i, d := range damages {
		out[i] = Hit{ID: uuid.New(), PlayerID: int64(i + 1), Damage: d}
	}
	return out
}

func TestApplyHits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		hp           int64
		damages      []int
		wantHP       int64
		wantAccepted int
		wantLate     int
		wantKillIdx  int // -1 — босс выжил
	}{
		{name: "empty batch", hp: 100, damages: nil, wantHP: 100, wantKillIdx: -1},
		{name: "boss survives", hp: 100, damages: []int{30, 20}, wantHP: 50, wantAccepted: 2, wantKillIdx: -1},
		{name: "killed mid-batch", hp: 100, damages: []int{60, 50, 40, 10}, wantHP: 0, wantAccepted: 2, wantLate: 2, wantKillIdx: 1},
		{name: "hp exactly zero is a kill", hp: 100, damages: []int{60, 40, 5}, wantHP: 0, wantAccepted: 2, wantLate: 1, wantKillIdx: 1},
		{name: "killed by last hit", hp: 100, damages: []int{50, 50}, wantHP: 0, wantAccepted: 2, wantKillIdx: 1},
		{name: "overkill by first hit", hp: 10, damages: []int{500, 1}, wantHP: 0, wantAccepted: 1, wantLate: 1, wantKillIdx: 0},
		{name: "already dead hp", hp: 0, damages: []int{5}, wantHP: 0, wantAccepted: 1, wantKillIdx: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hits := hitsOf(tc.damages...)
			got := ApplyHits(tc.hp, hits)

			if got.HPLeft != tc.wantHP {
				t.Errorf("HPLeft = %d, want %d", got.HPLeft, tc.wantHP)
			}
			if len(got.Accepted) != tc.wantAccepted || len(got.Late) != tc.wantLate {
				t.Errorf("accepted/late = %d/%d, want %d/%d",
					len(got.Accepted), len(got.Late), tc.wantAccepted, tc.wantLate)
			}
			if tc.wantKillIdx < 0 {
				if got.Killing != nil {
					t.Errorf("Killing = %+v, want nil", got.Killing)
				}
				return
			}
			if got.Killing == nil || got.Killing.ID != hits[tc.wantKillIdx].ID {
				t.Errorf("Killing = %+v, want hit #%d", got.Killing, tc.wantKillIdx)
			}
		})
	}
}

func TestApplyHits_PreservesOrder(t *testing.T) {
	t.Parallel()

	hits := hitsOf(10, 20, 30, 40)
	got := ApplyHits(55, hits)

	ids := func(hs []Hit) []uuid.UUID {
		out := make([]uuid.UUID, len(hs))
		for i, h := range hs {
			out[i] = h.ID
		}
		return out
	}
	if !slices.Equal(ids(got.Accepted), ids(hits[:3])) || !slices.Equal(ids(got.Late), ids(hits[3:])) {
		t.Fatalf("order broken: accepted=%v late=%v", ids(got.Accepted), ids(got.Late))
	}
}

func TestDedupHits(t *testing.T) {
	t.Parallel()

	hits := hitsOf(1, 2, 3)
	in := []Hit{hits[0], hits[1], hits[0], hits[2], hits[1]}

	got := DedupHits(in)
	if len(got) != 3 || got[0].ID != hits[0].ID || got[1].ID != hits[1].ID || got[2].ID != hits[2].ID {
		t.Fatalf("DedupHits = %+v", got)
	}
}

func TestGroupByRaid(t *testing.T) {
	t.Parallel()

	a, b := uuid.New(), uuid.New()
	in := hitsOf(1, 2, 3, 4, 5)
	in[0].RaidID, in[1].RaidID, in[2].RaidID, in[3].RaidID, in[4].RaidID = b, a, b, a, b

	got := GroupByRaid(in)
	if len(got) != 2 || got[0].RaidID != b || got[1].RaidID != a {
		t.Fatalf("group order: %+v", got)
	}
	if len(got[0].Hits) != 3 || got[0].Hits[0].Damage != 1 || got[0].Hits[1].Damage != 3 || got[0].Hits[2].Damage != 5 {
		t.Fatalf("hits order in group b: %+v", got[0].Hits)
	}
	if len(got[1].Hits) != 2 || got[1].Hits[0].Damage != 2 || got[1].Hits[1].Damage != 4 {
		t.Fatalf("hits order in group a: %+v", got[1].Hits)
	}
}

func TestAggregateDamage(t *testing.T) {
	t.Parallel()

	in := hitsOf(10, 20, 30)
	in[2].PlayerID = in[0].PlayerID

	got := AggregateDamage(in)
	want := []PlayerDamage{
		{PlayerID: in[0].PlayerID, Damage: 40, Hits: 2},
		{PlayerID: in[1].PlayerID, Damage: 20, Hits: 1},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("AggregateDamage = %+v, want %+v", got, want)
	}
}
