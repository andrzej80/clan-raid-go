package outbox

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
)

// Значения заголовка "type" у записей outbox.
const (
	TypeRaidDefeated  = "RaidDefeated"
	TypeRaidExpired   = "RaidExpired"
	TypeRewardGranted = "RewardGranted"
)

// DecodeJSON превращает бинарный payload записи outbox в JSON по заголовку "type".
// Неизвестный тип или битый protobuf — ошибка: вызывающий покажет сырые байты.
func DecodeJSON(headers map[string]string, payload []byte) ([]byte, error) {
	var msg proto.Message
	switch t := headers["type"]; t {
	case TypeRaidDefeated, TypeRaidExpired:
		msg = &eventsV1.RaidEvent{}
	case TypeRewardGranted:
		msg = &eventsV1.RewardGranted{}
	default:
		return nil, fmt.Errorf("unknown outbox record type %q", t)
	}
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", headers["type"], err)
	}
	out, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal json: %w", err)
	}
	return out, nil
}
