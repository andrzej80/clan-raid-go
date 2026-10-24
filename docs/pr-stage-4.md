## Этап 4. Redis: лидерборд, кэш HP, rate limit

### Что сделано

**Инфраструктура Redis** (`internal/storage/rdb`, `internal/config`)
- Один `redis.Client` на процесс, URL и таймауты из env (`REDIS_URL`, `REDIS_POOL_SIZE`, `REDIS_DIAL_TIMEOUT`, `REDIS_READ_TIMEOUT`, `REDIS_WRITE_TIMEOUT`).
- Все ключи собраны в `keys.go`: `rl:{player}`, `lb:{raid}`, `raid:{raid}:hp`, `raid:{raid}:updates`.

**Rate limit на Lua** (`internal/ratelimit`)
- `Redis`: token bucket одним скриптом. Время берётся через `TIME` внутри скрипта. Токены хранятся в микротокенах, поэтому задержка `RetryInfo` считается точно (200 мс при 5 уд/с). Ключ получает TTL, равный времени полного пополнения плюс запас.
- `Fallback`: Redis → `Memory` при его сбое. Circuit breaker: 3 ошибки подряд открывают его на 2 с, затем один пробный запрос. Отмена запроса клиентом сбоем Redis не считается. Метрика `raidboss.ratelimit.fallback`.
- В `api` лимит теперь общий для экземпляров (Redis) с резервом в памяти.

**Лидерборд и кэш HP** (`hits-consumer`)
- После коммита транзакции рейда `HitsProcessor` вызывает `BoardWriter.Apply`: `ZADD` абсолютных сумм (из `AddDamage`), `SET` HP, `PUBLISH` `WatchRaidResponse{hp_left}`.
- Ошибка Redis пачку не повторяет и не роняет: Postgres уже зафиксирован. Метрики `raidboss.consumer.board.duration` и `.errors`.

**`GetLeaderboard`** (`internal/raid/leaderboard.go`, `grpcapi`)
- Чтение из Redis с таймаутом 200 мс. Фоллбэк на Postgres (`raid_damage`) при ошибке Redis или когда лидерборд пуст, хотя рейд уже получил урон. Метрика `raidboss.leaderboard.fallback{reason}`.
- Фоллбэк идёт через `cache.TTL` (singleflight, TTL 1 с): не больше одного запроса к БД в секунду на пару (рейд, limit).
- `limit`: 0 → 10, максимум 100, отрицательный → `INVALID_ARGUMENT`; нет рейда → `NOT_FOUND`.

**`raidctl`**
- `rebuild-cache [--raid] [--chunk]`: лидерборд собирается во временном ключе и подменяется одним `RENAME` (читатели не видят пустой лидерборд), затем `PERSIST` и `SET` HP. Чужие ключи (rate limit) не трогаются, `FLUSHDB` не используется.
- `verify` проверяет инвариант 6 (лидерборд = `raid_damage`) и дополнительно HP-кэш = `raids.hp_left`. Включается, если задан `REDIS_URL`; `--redis=false` отключает.

**Сценарии и compose**
- `loadtest/redis_rebuild.sh`: нагрузка → `verify` → `FLUSHALL` → `verify` падает → `GetLeaderboard` отвечает из Postgres → `rebuild-cache` → `verify` проходит.
- `chaos_consumer.sh` проверяет Postgres без Redis (`verify --redis=false`), см. ограничения ниже.
- В compose у `hits-consumer` добавлены `REDIS_URL` и `depends_on: redis`.

### Что замерено

`loadtest/redis_rebuild.sh` на ноутбуке: 5 кланов × 50 игроков по 2 удара в секунду, 20 с.

| Показатель | Значение |
|---|---|
| RPC всего / rps | 8 251 / 413 |
| Латентность `Attack` | p50 1,3 мс, p95 5,0 мс, p99 38 мс |
| Лаг консьюмера после нагрузки | 0 |
| `verify` на живых данных (с Redis) | OK |
| После `FLUSHALL` | `verify`: 26 нарушений (инвариант 6 и HP) |
| `GetLeaderboard` при пустом Redis | ответ из Postgres, верный топ |
| `rebuild-cache` | 13 рейдов, 601 запись, 37 мс |
| `verify` после `rebuild-cache` | OK |

### Принятые решения

- **Redis не в health `api`.** `Attack` от него не зависит, лимит при сбое держит `Memory`, а `GetLeaderboard` есть чем заменить. Отступление от раздела 3 README: `SERVING` требует только Postgres и Kafka.
- **`REDIS_URL` в compose записан литералом**, как `KAFKA_BROKERS`: это внутренний адрес docker-сети, и подстановка из `.env` сломала бы контейнеры, если разработчик задаст там адрес для запуска с хоста.
- **Ошибка записи в Redis не повторяет пачку.** Повтор отсекается по `hit_id` и до записи в Redis не дойдёт, поэтому расхождение лечит только `rebuild-cache`.
- **Запись в Redis после `COMMIT`, а не внутри транзакции:** откат не должен оставлять в Redis данные, которых нет в Postgres.
- **Время в Lua берётся из Redis**, а тестовая подмена «сейчас» передаётся параметром скрипта (`ARGV[3]`), в проде он пуст.
- **Фоллбэк `Memory` при сбое Redis** лучше, чем fail open: лимит ослабляется до «на экземпляр», но не пропадает. В момент переключения у игрока в `Memory` полный bucket, то есть новый burst.
- **Нет индекса по `raid_damage(total_damage)`**: он обновлялся бы на каждый удар, а запрос фоллбэка читает только строки одного рейда.

### Известные ограничения

- Если консьюмер упал после `COMMIT`, но до записи в Redis, повторная пачка целиком состоит из дублей и Redis не обновляет. Состояние догонит следующий удар игрока (абсолютное значение), но у остальных игроков и HP расхождение останется до `rebuild-cache`. Поэтому `chaos_consumer.sh` не проверяет Redis. Возможное улучшение: при пачке из дублей перечитывать суммы из `raid_damage` и писать их в Redis.
- Порядок при равном уроне разный: Redis сортирует по `player_id` как по строке (по убыванию), Postgres по числу.
- Ключи Redis завершённых рейдов не истекают.
- Рейды, удалённые из Postgres, `rebuild-cache` в Redis не чистит.
- Тесты с `-race` не запускались локально: в окружении выключен cgo. Их прогонит CI.
- Rate limit на живом `api` под нагрузкой с остановленным Redis не проверялся: поведение покрыто тестами `Fallback`.

### Как проверить

```bash
go test ./...                       # интеграционные тесты требуют Docker (Postgres и Redis в контейнерах)
golangci-lint run ./...
loadtest/redis_rebuild.sh           # нужны pg, kafka и redis из deploy/; сбрасывает Redis из compose
```
