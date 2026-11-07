# Рейды не истекают

**Алерты:**
- `SchedulerErrors` (ticket): больше 3 ошибок `scheduler` за 10 мин;
- `ServiceNotReporting` для `scheduler` (ticket).

**Влияние.** Рейд, не убитый вовремя, остаётся `active` после `ends_at`. Удары после `ends_at` консьюмер всё равно считает опоздавшими, так что урон не засчитывается. Но клан не может начать новый рейд (правило «один активный рейд»), а подписчики `WatchRaid` не получают финал.

## Диагностика

1. Есть ли просроченные активные рейды:
   ```bash
   psql_q "SELECT count(*), min(ends_at) FROM raids WHERE status = 'active' AND ends_at < now() - interval '1 minute'"
   ```
2. Жив ли `scheduler` и что пишет:
   ```bash
   $DC ps scheduler
   docker exec scheduler wget -qO- http://127.0.0.1:8081/readyz
   docker logs --since 30m scheduler 2>&1 | grep -E "expire raids|error" | tail
   ```

## Действия

- **`scheduler` лежит** — `$DC up -d scheduler`. Он сразу выполнит проход и закроет все просроченные рейды.
- **Ошибки Postgres** — по классу ошибки, см. [consumer-partition-stuck.md](consumer-partition-stuck.md) (диагностика блокировок та же).
- Не переводить рейды в `expired` вручную через `UPDATE ... SET status`: так в outbox не попадёт событие истечения, а подписчики не получат финал. Как закрыть рейд руками — см. [manual-raid-finish.md](manual-raid-finish.md).

## Проверка

Запрос из п. 1 диагностики возвращает 0.

## Эскалация

Если `scheduler` работает, но рейды не истекают — разработчикам.

Отдельного алерта по просроченным рейдам нет: для этого нужна метрика из базы. Пока её роль выполняют `SchedulerErrors` и `ServiceNotReporting`.
