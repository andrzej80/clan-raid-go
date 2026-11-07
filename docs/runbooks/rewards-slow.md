# Награды выдаются с задержкой

**Алерт:** `RewardsSlow` (ticket): за час есть рейды, награды за которые записаны позже 30 с после победы.

**Влияние.** Игроки видят награды не сразу. Они не теряются: событие победы лежит в outbox или в Kafka.

## Диагностика

Награда проходит цепочку: консьюмер ударов пишет событие `RaidDefeated` в outbox → `outbox-relay` → топик `raid.events` → `rewards-worker` → таблица `rewards`. Найти звено, где она застряла:

1. Не разобран outbox — `raidboss_outbox_unsent` больше нуля; см. [outbox-backlog.md](outbox-backlog.md).
2. Отстаёт `rewards-worker`:
   ```bash
   kafka kafka-consumer-groups.sh --describe --group rewards-worker
   docker logs --since 30m rewards-worker 2>&1 | grep -E "will retry|dlq" | tail
   ```
3. Медленная транзакция наград — гистограмма `raidboss_rewards_duration_seconds`. Рейды с десятками тысяч участников считаются дольше.
4. Если победа была до перезапуска или сбоя, задержка выросла из-за простоя, а не из-за медленной обработки. Сверить время с алертами `ServiceNotReporting`.

## Действия

По найденному звену: [outbox-backlog.md](outbox-backlog.md), [consumer-partition-stuck.md](consumer-partition-stuck.md) (у `rewards-worker` тот же консьюмер), [service-not-reporting.md](service-not-reporting.md).

## Проверка

Новые победы получают награды быстрее 30 с, гистограмма `raidboss_rewards_delay_seconds` в норме.

## Эскалация

Если задержка повторяется без сбоев зависимостей — разработчикам `rewards-worker`.
