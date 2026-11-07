# Runbook'и

Инструкции на случай алертов (`deploy/alerts/`, аннотация `runbook_url`) и для плановых операций. Формат у всех одинаковый: признаки, влияние на игроков, диагностика, действия, как проверить, что помогло, когда эскалировать.

Команды написаны для стенда из `deploy/docker-compose.yml`. В Kubernetes (B7) `docker exec <сервис>` заменяется на `kubectl exec deploy/<сервис>`, а `docker logs` — на `kubectl logs`.

Общие переменные для команд ниже, задаются из корня репозитория:

```bash
set -a; . deploy/.env; set +a
DC="docker compose -f deploy/docker-compose.yml --env-file deploy/.env"
export DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
export REDIS_URL="redis://localhost:${REDIS_PORT}"
psql_q() { $DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"; }
kafka() { tool=$1; shift; docker exec kafka /opt/kafka/bin/"$tool" --bootstrap-server localhost:9092 "$@"; }
```

Где смотреть:
- дашборд Grafana «RaidBoss»: `http://localhost:${GRAFANA_PORT}`;
- активные алерты: `http://localhost:8880/vmalert/alerts`;
- пробы сервиса: `docker exec <сервис> wget -qO- http://127.0.0.1:8081/readyz`.

## По алертам

| Алерт | Severity | Runbook |
|---|---|---|
| `AttackErrorBudgetFastBurn`, `AttackErrorBudgetSlowBurn` | page / ticket | [attack-errors.md](attack-errors.md) |
| `AttackLatencyBudgetFastBurn` | page | [attack-latency.md](attack-latency.md) |
| `HitsFreshnessBudgetFastBurn`, `ConsumerLagHigh` | page | [consumer-lag.md](consumer-lag.md) |
| `ConsumerPartitionStuck`, `KafkaPartitionPausedLong` | page / ticket | [consumer-partition-stuck.md](consumer-partition-stuck.md) |
| `OutboxBacklog` | page | [outbox-backlog.md](outbox-backlog.md) |
| `DeadLetterRecords` | ticket | [dlq.md](dlq.md) |
| `RedisBoardWriteErrors` | ticket | [redis-divergence.md](redis-divergence.md) |
| `RewardsSlow` | ticket | [rewards-slow.md](rewards-slow.md) |
| `SchedulerErrors` | ticket | [raids-not-expiring.md](raids-not-expiring.md) |
| `PgPoolSaturated` | ticket | [pg-pool-saturated.md](pg-pool-saturated.md) |
| `ServiceNotReporting` | page / ticket | [service-not-reporting.md](service-not-reporting.md) |

## Плановые операции

- [Миграции схемы](migrations.md)
- [Откат релиза](rollback.md)
- [Добавление партиций raid.hits](kafka-add-partitions.md)
- [Пересоздание топика или кластера Kafka](kafka-topic-recreated.md)
- [Ручное завершение рейда](manual-raid-finish.md)
- [Ротация секретов](secret-rotation.md)
