# Redis расходится с Postgres

**Алерт:** `RedisBoardWriteErrors` (ticket): за 15 мин консьюмер не смог записать состояние рейда в Redis.

**Влияние.** Лидерборд, HP и число ударов на экране (`GetLeaderboard`, `WatchRaid`) отстают или неверны. Данные в Postgres верны: Redis — только кэш и pub/sub.

## Диагностика

1. Жив ли Redis:
   ```bash
   $DC ps redis
   $DC exec redis redis-cli ping
   ```
2. Ошибки записи в логе консьюмера:
   ```bash
   docker logs --since 30m hits-consumer 2>&1 | grep "redis board update failed" | tail
   ```
3. Насколько разошлось:
   ```bash
   go run ./cmd/raidctl verify
   ```
   Нарушения инвариантов Redis команда перечисляет отдельно.

## Действия

Восстановить Redis, затем пересобрать кэш из Postgres — либо для всех активных рейдов, либо для одного:

```bash
go run ./cmd/raidctl rebuild-cache
go run ./cmd/raidctl rebuild-cache --raid <uuid>
```

Повтор пачки сам Redis не починит: при повторе удары отсекаются как дубли, и до записи в Redis дело не доходит.

## Проверка

`raidctl verify` проходит, у клиентов `WatchRaid` HP совпадает с `GetRaid`.

## Эскалация

Если Redis не поднимается — владельцу Redis. Пока Redis лежит, приём ударов работает: rate limit переходит на локальный режим.
