package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	eventsV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/events/v1"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func TestWriteOutboxRecord_DecodesPayload(t *testing.T) {
	t.Parallel()

	payload, err := proto.Marshal(&eventsV1.RewardGranted{RaidId: "r1", PlayerId: 7, Kind: "top3", Amount: 500})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sent := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	rec := pg.OutboxRecord{
		ID: 3, Topic: "rewards.granted", Key: "7", Payload: payload,
		Headers:   map[string]string{"type": "RewardGranted"},
		CreatedAt: sent.Add(-time.Second),
		SentAt:    &sent,
	}

	var buf bytes.Buffer
	if err := writeOutboxRecord(&buf, rec); err != nil {
		t.Fatalf("writeOutboxRecord: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not one JSON object: %v: %q", err, buf.String())
	}
	body, _ := got["payload"].(map[string]any)
	if got["id"] != float64(3) || got["topic"] != "rewards.granted" || got["key"] != "7" ||
		body["kind"] != "top3" || body["amount"] != float64(500) || got["decode_error"] != nil {
		t.Fatalf("record = %v", got)
	}
	if got["sent_at"] == nil {
		t.Fatalf("sent_at missing: %v", got)
	}
	if buf.Bytes()[buf.Len()-1] != '\n' || bytes.Count(buf.Bytes(), []byte("\n")) != 1 {
		t.Fatalf("each record must be exactly one line: %q", buf.String())
	}
}

func TestWriteOutboxRecord_UnknownTypeShowsRawBytes(t *testing.T) {
	t.Parallel()

	rec := pg.OutboxRecord{
		ID: 1, Topic: "t", Key: "k", Payload: []byte{1, 2, 3},
		Headers:   map[string]string{"type": "Mystery"},
		CreatedAt: time.Now(),
	}

	var buf bytes.Buffer
	if err := writeOutboxRecord(&buf, rec); err != nil {
		t.Fatalf("writeOutboxRecord: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}
	if got["payload_base64"] != "AQID" || got["decode_error"] == nil || got["payload"] != nil || got["sent_at"] != nil {
		t.Fatalf("record = %v, want raw bytes, decode_error and sent_at null", got)
	}
}
