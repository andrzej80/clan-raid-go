package app

import (
	"context"
	"time"

	"github.com/andrzej80/clan-raid-go/internal/config"
	"github.com/andrzej80/clan-raid-go/internal/health"
)

// Таймауты проверок readiness. У зависимостей разное время отклика: Postgres в той же сети
// отвечает за миллисекунды, Kafka при первом обращении обновляет метаданные.
const (
	PostgresPingTimeout = 500 * time.Millisecond
	KafkaPingTimeout    = 2 * time.Second
)

// Pinger — зависимость, которую можно проверить.
type Pinger interface {
	Ping(ctx context.Context) error
}

// PostgresCheck — проверка readiness для пула Postgres.
func PostgresCheck(p Pinger) health.Check {
	return health.Check{Name: "postgres", Timeout: PostgresPingTimeout, Ping: p.Ping}
}

// KafkaCheck — проверка readiness для клиента Kafka.
func KafkaCheck(p Pinger) health.Check {
	return health.Check{Name: "kafka", Timeout: KafkaPingTimeout, Ping: p.Ping}
}

// StartProbes запускает HTTP-сервер проб (/livez, /readyz, pprof) на cfg.Addr. hb — отметка
// главного цикла; nil, если цикла нет. Сервер работает до отмены ctx или вызова возвращённой
// функции; её вызывают через defer: она останавливает сервер и ждёт его завершения, в том числе
// когда run выходит по ошибке, не дожидаясь сигнала.
func StartProbes(ctx context.Context, cfg config.ProbeConfig, hb *health.Heartbeat, checks ...health.Check) (func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	wait, err := health.Serve(ctx, cfg.Addr, health.Handler(health.Config{
		Heartbeat: hb,
		MaxStall:  cfg.MaxStall,
		Checks:    checks,
	}))
	if err != nil {
		cancel()
		return nil, err
	}
	return func() {
		cancel()
		wait()
	}, nil
}
