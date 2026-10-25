package outbox

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
)

func TestDecodeJSON(t *testing.T) {
	t.Parallel()

	mustMarshal := func(m proto.Message) []byte {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}
	defeated := mustMarshal(&eventsV1.RaidEvent{
		RaidId: "r1",
		Event:  &eventsV1.RaidEvent_Defeated{Defeated: &eventsV1.RaidDefeated{KillerPlayerId: 7, KillerHitId: "h1"}},
	})
	expired := mustMarshal(&eventsV1.RaidEvent{
		RaidId: "r2",
		Event:  &eventsV1.RaidEvent_Expired{Expired: &eventsV1.RaidExpired{HpLeft: 10}},
	})
	reward := mustMarshal(&eventsV1.RewardGranted{RaidId: "r1", PlayerId: 7, Kind: "top3", Amount: 500})

	tests := []struct {
		name    string
		typ     string
		payload []byte
		want    map[string]any
		wantErr bool
	}{
		{
			name: "defeated", typ: TypeRaidDefeated, payload: defeated,
			want: map[string]any{
				"raid_id":  "r1",
				"defeated": map[string]any{"killer_player_id": "7", "killer_hit_id": "h1"},
			},
		},
		{
			name: "expired", typ: TypeRaidExpired, payload: expired,
			want: map[string]any{"raid_id": "r2", "expired": map[string]any{"hp_left": "10"}},
		},
		{
			name: "reward", typ: TypeRewardGranted, payload: reward,
			want: map[string]any{"raid_id": "r1", "player_id": "7", "kind": "top3", "amount": float64(500)},
		},
		{name: "unknown type", typ: "Mystery", payload: reward, wantErr: true},
		{name: "no type", typ: "", payload: reward, wantErr: true},
		{name: "garbage", typ: TypeRaidDefeated, payload: []byte("\xff\xff not protobuf"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := DecodeJSON(map[string]string{"type": tc.typ}, tc.payload)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DecodeJSON error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("result is not JSON: %v: %s", err, raw)
			}
			for k, want := range tc.want {
				if !jsonEqual(got[k], want) {
					t.Fatalf("%s = %v, want %v (json: %s)", k, got[k], want, raw)
				}
			}
		})
	}
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
