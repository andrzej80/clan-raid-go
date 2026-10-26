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

// RaidEventProcessor обрабатывает пачку событий рейдов. Ошибка — повторить пачку целиком;
// отклонённые события консьюмер переносит в DLQ.
type RaidEventProcessor interface {
	ProcessEvents(ctx context.Context, events []domain.RaidEvent) ([]domain.RejectedEvent, error)
}

// NewRaidEventsConsumer создаёт консьюмер топика raid.events: записи разбираются как RaidEvent.
func NewRaidEventsConsumer(cfg ConsumerConfig, processor RaidEventProcessor, extra ...kgo.Opt) (*Consumer, error) {
	return NewBatchConsumer(cfg, raidEventsHandler{processor: processor}, extra...)
}

// raidEventsHandler разбирает записи как RaidEvent и передаёт события RaidEventProcessor.
type raidEventsHandler struct {
	processor RaidEventProcessor
}

func (h raidEventsHandler) HandleBatch(ctx context.Context, records []*kgo.Record) (dead []DLQRecord, err error) {
	ctx, bt := startBatchTrace(ctx, "raid.events process batch", records)
	defer func() { bt.end(err) }()

	events := make([]domain.RaidEvent, 0, len(records))
	sources := make([]*kgo.Record, 0, len(records)) // sources[i] — запись события events[i]

	for _, rec := range records {
		ev, err := decodeRaidEvent(rec.Value)
		if err != nil {
			dead = append(dead, DLQRecord{Record: rec, Reason: dlqReasonDecode, Details: err.Error()})
			continue
		}
		events = append(events, ev)
		sources = append(sources, rec)
		bt.record(rec, "raid.events process event", attribute.String("raid_id", ev.RaidID.String()))
	}
	if len(events) == 0 {
		return dead, nil
	}

	rejected, err := h.processor.ProcessEvents(ctx, events)
	if err != nil {
		return nil, err
	}
	for _, r := range rejected {
		dead = append(dead, DLQRecord{Record: sources[r.Index], Reason: dlqReasonRejected, Details: r.Reason})
	}
	return dead, nil
}

// decodeRaidEvent разбирает и проверяет RaidEvent. Любая ошибка — повод отправить запись в DLQ.
func decodeRaidEvent(value []byte) (domain.RaidEvent, error) {
	var ev eventsV1.RaidEvent
	if err := proto.Unmarshal(value, &ev); err != nil {
		return domain.RaidEvent{}, fmt.Errorf("unmarshal raid event: %w", err)
	}
	raidID, err := uuid.Parse(ev.GetRaidId())
	if err != nil {
		return domain.RaidEvent{}, fmt.Errorf("raid_id: %w", err)
	}

	out := domain.RaidEvent{RaidID: raidID}
	switch ev.GetEvent().(type) {
	case *eventsV1.RaidEvent_Defeated:
		out.Kind = domain.RaidEventDefeated
	case *eventsV1.RaidEvent_Expired:
		out.Kind = domain.RaidEventExpired
	default:
		return domain.RaidEvent{}, errors.New("event type is not set")
	}
	return out, nil
}
