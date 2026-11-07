# Outbox не разбирается

**Алерт:** `OutboxBacklog` (page): больше 100 неотправленных записей outbox 10 мин.

**Влияние.** События побед и наград не уходят в Kafka. Награды выдаются с задержкой, внешние потребители `rewards.granted` их не видят. Записи не теряются: они лежат в таблице `outbox`.

## Диагностика

1. Проверить, работает ли relay и что пишет в лог:
   ```bash
   $DC ps outbox-relay
   docker logs --since 10m "$(docker ps -qf name=outbox-relay | head -1)" 2>&1 | grep -E "relay batch|will retry" | tail
   ```
2. Проверить, доступна ли Kafka (см. [attack-errors.md](attack-errors.md)) и готов ли relay (`/readyz`).
3. Посмотреть, что застряло:
   ```bash
   go run ./cmd/raidctl outbox dump --unsent --limit 20
   ```
4. Найти долгие транзакции relay в Postgres. Пачка держит транзакцию на время отправки, но не дольше 30 с:
   ```bash
   psql_q "SELECT pid, now() - xact_start AS age, left(query, 80) FROM pg_stat_activity WHERE query ILIKE '%outbox%' AND state <> 'idle'"
   ```

## Действия

- **Kafka недоступна** — восстановить её, relay догонит сам.
- **Relay не запущен** — `$DC up -d outbox-relay`.
- **Не публикуется одна запись** (например, она больше `max.message.bytes`). Её `id` виден в выводе `outbox dump`. Передать разработчикам: чинить вручную, не понимая причины, нельзя.

## Проверка

`raidboss_outbox_unsent` уменьшается до 0, награды появляются в `ListRewards`.

## Эскалация

Если outbox не разбирается 30 мин при живой Kafka — разработчикам relay.
