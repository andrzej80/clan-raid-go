package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// Причины отправки в DLQ (метка reason у счётчика).
const (
	dlqReasonDecode   = "decode"   // сообщение не разобралось как HitEvent
	dlqReasonRejected = "rejected" // удар валиден, но применить его нельзя (нет игрока или рейда)
)

// hitsHandler разбирает записи как HitEvent и передаёт удары HitProcessor.
type hitsHandler struct {
	processor HitProcessor
}

func (h hitsHandler) HandleBatch(ctx context.Context, records []*kgo.Record) (dead []DLQRecord, err error) {
	ctx, bt := startBatchTrace(ctx, "raid.hits process batch", records)
	defer func() { bt.end(err) }()

	hits := make([]domain.Hit, 0, len(records))
	bySource := make(map[uuid.UUID]*kgo.Record, len(records))

	for _, rec := range records {
		hit, err := decodeHit(rec.Value)
		if err != nil {
			dead = append(dead, DLQRecord{Record: rec, Reason: dlqReasonDecode, Details: err.Error()})
			continue
		}
		hits = append(hits, hit)
		bySource[hit.ID] = rec
		bt.record(rec, "raid.hits process hit",
			attribute.String("hit_id", hit.ID.String()), attribute.String("raid_id", hit.RaidID.String()))
	}
	if len(hits) == 0 {
		return dead, nil
	}

	result, err := h.processor.ProcessBatch(ctx, hits)
	if err != nil {
		return nil, err
	}
	for _, rejected := range result.Rejected {
		for _, hit := range rejected.Hits {
			dead = append(dead, DLQRecord{Record: bySource[hit.ID], Reason: dlqReasonRejected, Details: rejected.Reason})
		}
	}
	return dead, nil
}

// decodeHit разбирает и проверяет HitEvent. Любая ошибка — повод отправить запись в DLQ.
func decodeHit(value []byte) (domain.Hit, error) {
	var ev eventsV1.HitEvent
	if err := proto.Unmarshal(value, &ev); err != nil {
		return domain.Hit{}, fmt.Errorf("unmarshal hit event: %w", err)
	}
	hitID, err := uuid.Parse(ev.GetHitId())
	if err != nil {
		return domain.Hit{}, fmt.Errorf("hit_id: %w", err)
	}
	raidID, err := uuid.Parse(ev.GetRaidId())
	if err != nil {
		return domain.Hit{}, fmt.Errorf("raid_id: %w", err)
	}
	if ev.GetPlayerId() <= 0 || ev.GetDamage() <= 0 || ev.GetCreatedAt() == nil {
		return domain.Hit{}, errors.New("player_id, damage and created_at must be set")
	}
	return domain.Hit{
		ID:        hitID,
		RaidID:    raidID,
		PlayerID:  ev.GetPlayerId(),
		ClanID:    ev.GetClanId(),
		Damage:    int(ev.GetDamage()),
		IsCrit:    ev.GetIsCrit(),
		CreatedAt: ev.GetCreatedAt().AsTime(),
	}, nil
}
