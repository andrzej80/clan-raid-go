# Attack отвечает ошибками

**Алерты:** `AttackErrorBudgetFastBurn` (page), `AttackErrorBudgetSlowBurn` (ticket).

**Признаки.** На панели «RPS по RPC и коду» у `Attack` растут ответы `UNAVAILABLE`, `DEADLINE_EXCEEDED` или `INTERNAL`. `RESOURCE_EXHAUSTED` (rate limit) в SLI не входит.

**Влияние.** Удары игроков не принимаются. Клиент повторяет запрос с тем же `idempotency_key`, поэтому кратковременный сбой обычно не теряет удары, но игроки видят задержку.

## Диагностика

1. Определить код ответа. `UNAVAILABLE` почти всегда значит, что очередь (Kafka) недоступна и предохранитель в `api` разомкнут:
   ```bash
   docker logs --since 10m api 2>&1 | grep -E "circuit breaker|publish hit|Health status changed" | tail -20
   ```
2. Проверить, готов ли `api` и доступны ли его зависимости:
   ```bash
   docker exec api wget -qO- http://127.0.0.1:8081/readyz
   ```
3. Проверить Kafka: живы ли брокеры и есть ли у партиций лидер.
   ```bash
   $DC ps kafka
   kafka kafka-topics.sh --describe --topic raid.hits --unavailable-partitions
   ```
4. При `INTERNAL` найти ошибку в логе `api` по `trace_id` и открыть трейс в Tempo.

## Действия

- **Kafka недоступна.** Поднять брокер: `$DC up -d kafka`. Предохранитель сам переходит в half-open после успешного `Ping` и замыкается по пробам, перезапускать `api` не нужно.
- **Kafka пересоздана** (топики созданы заново). Клиент в `api` пересозданный топик не подхватит — см. [kafka-topic-recreated.md](kafka-topic-recreated.md).
- **Потерян один брокер из трёх** (`docker-compose.kafka-ha.yml`). Запись во все партиции стоит, пока контроллер не исключит брокер (`broker.session.timeout.ms`, по умолчанию 9 с); это ожидаемо. Если сбой длится дольше, выяснить, почему не переезжают лидеры (`--unavailable-partitions` в п. 3).
- **Сбой начался после релиза** — см. [rollback.md](rollback.md).

## Проверка

Доля ошибок на панели вернулась к нулю, `raidboss:attack_error_ratio:rate5m` ниже 0,0144. После восстановления запустить `go run ./cmd/raidctl verify`: удары с успешным ответом не должны потеряться.

## Эскалация

Если Kafka не поднимается за 15 мин или сбой не объясняется Kafka — владельцу кластера Kafka и разработчикам `api`.
