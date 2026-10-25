## Этап 5. Outbox, награды, истечение рейдов

### Что сделано

**`outbox-relay`** (`internal/outbox`, `internal/kafka/publisher.go`, `cmd/outbox-relay`)
- Цикл из 6.3: в одной транзакции `SELECT ... FOR UPDATE SKIP LOCKED` (пачка 100, порядок по `id`), синхронная отправка в Kafka, `UPDATE sent_at`, `COMMIT`. Несколько экземпляров не забирают одни и те же строки.
- `kafka.Publisher` берёт топик, ключ и заголовки из самой записи (`headers` JSONB → заголовки записи Kafka, отсортированы по ключу). Подтверждение от всех реплик, идемпотентная запись.
- Ошибка отправки откатывает транзакцию: записи остаются неотправленными, цикл повторяет с backoff 100 мс → 5 с. Пустой outbox — пауза `OUTBOX_POLL_INTERVAL`, непустой — следующая пачка сразу.
- Сигнал остановки не обрывает начатую пачку (контекст пачки отвязан через `context.WithoutCancel`): иначе после публикации запись осталась бы неотмеченной и ушла бы повторно. Предел ожидания задаёт таймаут доставки Kafka (5 с).
- Метрики: `raidboss.outbox.published`, `.errors`, `.batch.duration`, gauge `raidboss.outbox.unsent`.

**Награды** (`internal/rewards`, `internal/kafka/events_handler.go`, `cmd/rewards-worker`)
- `kafka.Consumer` обобщён: интерфейс `BatchHandler` (разобрать пачку, вернуть записи для DLQ, ошибка — повтор). `NewConsumer` для ударов остался с прежней сигнатурой, его тесты прошли без правок. Новый `NewRaidEventsConsumer` разбирает `RaidEvent`.
- Для `defeated` в одной транзакции: статус рейда и убийца из Postgres → урон из `raid_damage` → `CalcRewards` → `INSERT ... ON CONFLICT DO NOTHING RETURNING` → `RewardGranted` в outbox только для реально вставленных строк (ключ `player_id`). `expired` пропускается.
- Рейд не `defeated` или не найден: событие уходит в `raid.events.dlq` с причиной, остальные события пачки обрабатываются.
- Метрики: `raidboss.rewards.events{outcome}`, `.granted{kind}`, `.duration`.

**Истечение** (`internal/scheduler`, `cmd/scheduler`)
- Раз в `SCHEDULER_INTERVAL` (10 с, первый проход сразу): `UPDATE raids SET status='expired', finished_at=now() WHERE status='active' AND ends_at < now() RETURNING ...` и `RaidEvent{expired}` в outbox, одна транзакция. Время берётся из Postgres, как и `ends_at`.
- Несколько экземпляров безопасны благодаря `status = 'active'`. Если консьюмер ударов держит строку рейда, `UPDATE` ждёт и перепроверяет условие: рейд, добитый последним ударом, остаётся `defeated` (тест).

**Удары после `ends_at`** (`domain.SplitByDeadline`, `HitsRepo.LockRaid`)
- `LockRaid` возвращает `ends_at`. Консьюмер считает опоздавшими удары с `created_at >= ends_at`, даже если `scheduler` ещё не закрыл рейд: босс не может умереть после конца рейда (правило 6). Опоздавшие по времени идут в ту же метрику `late`.

**`ListRewards`**
- Игрок берётся из `x-player-id`, награды новые первыми, не больше 1000. Неизвестный игрок — `NOT_FOUND`, нет заголовка — `UNAUTHENTICATED`.
- Поле `player_id` запроса помечено `deprecated` и игнорируется: чужие награды по подставленному id получить нельзя.

**`raidctl`**
- `outbox dump [--unsent] [--topic] [--key] [--limit]`: JSON lines, payload расшифрован через `protojson` по заголовку `type`. Неизвестный тип или битый protobuf — сырые байты в `payload_base64` и причина в `decode_error`.
- `verify`: инвариант 5 — награды только у `defeated` рейдов и не больше одной на `(raid, player, kind)`; награды `defeated` рейда совпадают с пересчётом `CalcRewards`. Флаг `--require-rewards` дополнительно требует награды у каждого `defeated` рейда (после того как `rewards-worker` разобрал очередь).

**Инфраструктура**
- Миграция: индекс `raids(ends_at) WHERE status='active'` и `rewards(player_id, id DESC)`.
- compose: `outbox-relay` (без `container_name`, масштабируется `--scale`), `rewards-worker`, `scheduler`; топик `raid.events.dlq`.
- Конфиг: `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL`, `SCHEDULER_INTERVAL`, `KAFKA_EVENTS_TOPIC`, `KAFKA_EVENTS_DLQ_TOPIC`, `KAFKA_REWARDS_TOPIC`, `REWARDS_CONSUMER_GROUP`. Нули и отрицательные значения заменяются умолчаниями.

### Что замерено

`loadtest/outbox_relays.sh` на ноутбуке: 5 кланов × 50 игроков по 2 удара в секунду, `hp=500000`, три `outbox-relay`, плюс рейд, который истекает через 15 с.

| Показатель | Значение |
|---|---|
| RPC всего / rps (все 5 боссов умерли за ~10 с) | 4 819 / 472 |
| Латентность `Attack` | p50 3,3 мс, p95 22,9 мс, p99 44,7 мс |
| Записей в outbox отправлено / записей в Kafka | 721 / 721 (дублей нет) |
| То же с `CHAOS=1` (`docker kill` одного relay посреди нагрузки) | 997 / 997, потерь нет |
| От смерти босса до первой награды в БД (10 рейдов) | среднее 172 мс, максимум 230 мс |
| Рейд с `ends_at` через 15 с | стал `expired`, наград нет |
| `raidctl verify --require-rewards` | OK после обоих запусков |

В числа outbox входят события, накопленные до этапа 5 (relay не было): первый запуск отправил и их. Время до награды в основном складывается из паузы опроса relay (200 мс) и работы `rewards-worker`.

### Принятые решения

- **Ориентир на `ends_at` в консьюмере.** Альтернатива, оставить погрешность до 10 с (интервал `scheduler`), нарушала бы правило 6. Цена: удар, созданный до `ends_at`, но обработанный после перехода рейда в `expired`, считается опоздавшим. При нулевом лаге консьюмера такое возможно только в пределах секунд вокруг `ends_at`.
- **`created_at` ставит `api`, `ends_at` и `now()` — Postgres.** Расхождение часов даёт погрешность порядка секунд на границе рейда. Для учебного проекта принято.
- **Убийца берётся из `raids.killer_player_id`, а не из события.** Postgres — источник правды, событие нужно только как триггер.
- **`player_id` в `ListRewardsRequest` не удалён, а помечен `deprecated`.** Удаление поля роняет `buf breaking` (категория `FILE`, правило `FIELD_NO_DELETE`), а менять категорию ради одного поля, которым никто не пользовался, избыточно.
- **`outbox dump --key`, а не `--raid`.** Ключ записи разный: `raid_id` у `raid.events`, `player_id` у `rewards.granted`.
- **Глобальный порядок событий не гарантируется** при нескольких relay. Порядок внутри рейда не важен: у рейда не бывает двух событий (`defeated` и `expired` взаимоисключающи), а награды независимы друг от друга.
- **Relay держит соединение с БД на время отправки в Kafka.** Это цена того, что блокировка `SKIP LOCKED` живёт вместе с транзакцией; поэтому `PG_MAX_CONNS=4` у relay достаточно.
- **DLQ записей консьюмера отправляются после успешной обработки пачки**, а не до неё: при повторах пачки DLQ не получает дублей.

### Известные ограничения

- Таблица `outbox` не чистится: отправленные записи копятся. Нужен retention (удаление `sent_at < now() - N`).
- At-least-once: если relay упал после отправки, но до `COMMIT`, запись уйдёт повторно. В прогоне с убитым relay дублей не наблюдалось (убийство пришлось не на отправку), поэтому дубликаты проверены только тестами идемпотентности `rewards-worker`, а не на живой Kafka.
- `ListRewards` без пагинации: максимум 1000 наград.
- Награды для рейдов, убитых до этапа 5, выдаются при первом запуске `outbox-relay` и `rewards-worker` (в outbox лежали неотправленные события). Первый запуск сценария в dev-окружении выдал их, а `scheduler` перевёл просроченные старые рейды в `expired`.
- Тесты с `-race` не запускались локально: в окружении выключен cgo. Их прогонит CI.
- `rewards-worker`, `outbox-relay` и `scheduler` не отдают health-эндпоинта.

### Как проверить

```bash
go test ./...                       # интеграционные тесты требуют Docker (Postgres в контейнере)
golangci-lint run ./...
loadtest/outbox_relays.sh           # нужны pg, kafka и redis из deploy/; добавляет данные и перезапускает api и hits-consumer
CHAOS=1 loadtest/outbox_relays.sh   # то же, с убийством одного relay посреди нагрузки
raidctl outbox dump --unsent        # что ещё не ушло в Kafka
raidctl verify --require-rewards
```
