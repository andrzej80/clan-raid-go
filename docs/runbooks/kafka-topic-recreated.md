# Пересоздание топика или кластера Kafka

**Когда:** топик удалили и создали заново, или кластер Kafka пересоздан без данных. Например, compose пересоздал контейнер Kafka после смены его настроек, а `kafka-init` заново создал топики.

## Что ломается

У пересозданного топика новый ID. Клиент franz-go в уже запущенном сервисе такой топик сам не подхватывает — это описано в документации franz-go. Запись из `api` и чтение консьюмерами не проходят совсем: на прогоне без перезапуска `api` не принял ни одного удара. Алерты при этом — `AttackErrorBudgetFastBurn` и `ServiceNotReporting` либо `ConsumerLagHigh`.

## Перед удалением топика

Удаление `raid.hits` с непрочитанными записями теряет удары, на которые клиент уже получил успешный ответ. Сначала дождаться лага 0:

```bash
kafka kafka-consumer-groups.sh --describe --group hits-consumer
kafka kafka-consumer-groups.sh --describe --group rewards-worker
```

Outbox при этом не теряется: неотправленное лежит в Postgres, и relay опубликует его в новый топик.

## После пересоздания

1. Создать топики по описанию в коде и проверить их:
   ```bash
   $DC up --build --no-deps kafka-init
   go run ./cmd/raidctl topics diff
   ```
2. Перезапустить все сервисы, работающие с Kafka:
   ```bash
   $DC up -d --force-recreate --no-deps api hits-consumer outbox-relay rewards-worker
   ```
   Offset'ы групп пропали вместе с кластером, поэтому консьюмеры начнут новые топики с начала.

## Проверка

`Attack` отвечает `OK`, лаг консьюмеров около 0, `go run ./cmd/raidctl verify` проходит.
