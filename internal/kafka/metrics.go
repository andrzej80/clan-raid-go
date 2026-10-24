package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/andrzej80/clan-raid-go/internal/kafka"

// Значения атрибута reason у счётчика ошибок.
const (
	reasonBroker         = "broker"          // брокер недоступен или отклонил запись
	reasonClientDeadline = "client_deadline" // истёк дедлайн клиента
	reasonClientCanceled = "client_canceled" // клиент отменил запрос
	reasonEncode         = "encode"          // не удалось сериализовать событие
)

type producerMetrics struct {
	errors   metric.Int64Counter
	duration metric.Float64Histogram
}

func newProducerMetrics(mp metric.MeterProvider) (*producerMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	errs, err := meter.Int64Counter("raidboss.kafka.produce.errors",
		metric.WithDescription("Failed attempts to publish a hit to Kafka"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("create errors counter: %w", err)
	}
	dur, err := meter.Float64Histogram("raidboss.kafka.produce.duration",
		metric.WithDescription("Time from Publish call to broker acknowledgement or failure"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create duration histogram: %w", err)
	}
	return &producerMetrics{errors: errs, duration: dur}, nil
}

// observe фиксирует итог одной отправки; reason пустой при успехе.
func (m *producerMetrics) observe(ctx context.Context, start time.Time, reason string) {
	result := "ok"
	if reason != "" {
		result = "error"
		m.errors.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	}
	m.duration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("result", result)))
}

// failureReason отличает ошибки клиента (дедлайн, отмена) от сбоев брокера.
func failureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return reasonClientDeadline
	case errors.Is(err, context.Canceled):
		return reasonClientCanceled
	default:
		return reasonBroker
	}
}

// Значения атрибута reason у счётчика DLQ.
const (
	dlqReasonDecode   = "decode"   // сообщение не разобралось как HitEvent
	dlqReasonRejected = "rejected" // удар валиден, но применить его нельзя (нет игрока или рейда)
)

type consumerMetrics struct {
	dlq          metric.Int64Counter
	batchRetries metric.Int64Counter
}

func newConsumerMetrics(mp metric.MeterProvider) (*consumerMetrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	dlq, err := meter.Int64Counter("raidboss.kafka.consume.dlq",
		metric.WithDescription("Records sent to the dead-letter topic"),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, fmt.Errorf("create dlq counter: %w", err)
	}
	retries, err := meter.Int64Counter("raidboss.kafka.consume.batch.retries",
		metric.WithDescription("Failed attempts to process a batch (the batch is retried)"),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, fmt.Errorf("create batch retries counter: %w", err)
	}
	return &consumerMetrics{dlq: dlq, batchRetries: retries}, nil
}
