package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/outbox"
	"github.com/andrzej80/clan-raid-go/internal/storage/pg"
)

func runOutbox(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "dump" {
		return fmt.Errorf("outbox: subcommand is required (dump)\n\n%s", usage)
	}
	return runOutboxDump(ctx, args[1:], out)
}

func runOutboxDump(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("outbox dump", flag.ContinueOnError)
	unsent := fs.Bool("unsent", false, "only records not yet published to Kafka")
	topic := fs.String("topic", "", "only records for this topic")
	key := fs.String("key", "", "only records with this key (raid id for raid.events, player id for rewards.granted)")
	limit := fs.Int("limit", 50, "how many of the latest matching records to print")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *limit < 1 {
		return errors.New("--limit must be positive")
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	pool, err := pg.NewPool(ctx, &cfg.DB)
	if err != nil {
		return err
	}
	defer pool.Close()

	records, err := pg.NewOutboxRepo(pool).List(ctx, pg.OutboxFilter{
		Unsent: *unsent, Topic: *topic, Key: *key, Limit: *limit,
	})
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := writeOutboxRecord(out, rec); err != nil {
			return err
		}
	}
	return nil
}

// outboxLine — одна строка вывода dump (JSON lines). Payload расшифрован через protojson;
// если тип неизвестен или payload битый, вместо него печатаются сырые байты и причина.
type outboxLine struct {
	ID            int64             `json:"id"`
	Topic         string            `json:"topic"`
	Key           string            `json:"key"`
	Headers       map[string]string `json:"headers"`
	CreatedAt     time.Time         `json:"created_at"`
	SentAt        *time.Time        `json:"sent_at"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
	PayloadBase64 string            `json:"payload_base64,omitempty"`
	DecodeError   string            `json:"decode_error,omitempty"`
}

func writeOutboxRecord(out io.Writer, rec pg.OutboxRecord) error {
	line := outboxLine{
		ID: rec.ID, Topic: rec.Topic, Key: rec.Key, Headers: rec.Headers,
		CreatedAt: rec.CreatedAt, SentAt: rec.SentAt,
	}
	if payload, err := outbox.DecodeJSON(rec.Headers, rec.Payload); err != nil {
		line.PayloadBase64 = base64.StdEncoding.EncodeToString(rec.Payload)
		line.DecodeError = err.Error()
	} else {
		line.Payload = payload
	}

	encoded, err := json.Marshal(line)
	if err != nil {
		return fmt.Errorf("encode outbox record %d: %w", rec.ID, err)
	}
	_, err = fmt.Fprintln(out, string(encoded))
	return err
}
