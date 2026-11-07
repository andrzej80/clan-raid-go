# Attack отвечает медленно

**Алерт:** `AttackLatencyBudgetFastBurn` (page): больше 14,4% успешных ответов `Attack` дольше 50 мс.

**Влияние.** Удары принимаются, но с задержкой. Клиент с коротким дедлайном получает `DEADLINE_EXCEEDED`.

## Диагностика

1. На панели «p99 по RPC» определить, растёт только `Attack` или все RPC. Все — проблема в `api` (CPU, GC, пул соединений); только `Attack` — в Kafka.
2. Посмотреть время подтверждения брокером (гистограмма `raidboss_kafka_produce_duration_seconds`) и ошибки продюсера (панель «Ошибки продюсера»).
3. Проверить нагрузку на `api`: `go_goroutine_count{job="api"}`, `go_memory_used_bytes`. При необходимости снять профиль CPU:
   ```bash
   docker exec api wget -qO- "http://127.0.0.1:8081/debug/pprof/profile?seconds=20" > cpu.pprof
   go tool pprof -top cpu.pprof | head -30
   ```
4. При промахе кэша игроков и рейдов `Attack` ходит в Postgres — проверить пул `api` на панели «Занятость пула».

## Действия

- **Медленный брокер** (нагрузка, диск) — разгрузить его или добавить брокеры. Если у партиций нет лидера — см. [attack-errors.md](attack-errors.md).
- **Перегружен `api`** — добавить экземпляр. Клиенты перераспределятся через `MaxConnectionAge`.
- **Голодание пула `api`** — см. [pg-pool-saturated.md](pg-pool-saturated.md).

## Проверка

`raidboss:attack_slow_ratio:rate5m` ниже 0,144, p99 `Attack` на панели вернулся к норме (единицы миллисекунд).

## Эскалация

Если причина не найдена за 30 мин — разработчикам `api`.
