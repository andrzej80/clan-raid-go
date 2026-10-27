#!/usr/bin/env bash
# Хаос этапа 8: остановить Kafka на 30 с посреди нагрузки.
#
# Ожидается: пока брокера нет, Attack отвечает UNAVAILABLE, клиент повторяет с тем же
# idempotency_key, после возвращения брокера удары снова принимаются, консьюмер догоняет лаг,
# дублей ударов нет (raid_hits.hit_id — PK), raidctl verify проходит.
#
#   loadtest/chaos_kafka.sh
#   KAFKA_DOWNTIME=30 KILL_AFTER=5 DURATION=70s loadtest/chaos_kafka.sh
set -euo pipefail

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
HP=${HP:-3000000}         # боссы живут всю нагрузку
DURATION=${DURATION:-70s}  # больше KILL_AFTER + KAFKA_DOWNTIME, чтобы проверить и восстановление
KILL_AFTER=${KILL_AFTER:-5}
KAFKA_DOWNTIME=${KAFKA_DOWNTIME:-30}

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"
ON_EXIT='docker start kafka >/dev/null 2>&1'

build_tools
infra_up
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer rewards-worker scheduler outbox-relay

start_load chaos-kafka
sleep "$KILL_AFTER"
step "CHAOS: stop kafka for ${KAFKA_DOWNTIME}s"
docker stop kafka >/dev/null
sleep "$KAFKA_DOWNTIME"
docker start kafka >/dev/null
$DC up -d --wait kafka
finish_load

unavailable=$(report_count Unavailable)
echo "UNAVAILABLE responses seen by the client: ${unavailable:-0}"
if [ "${unavailable:-0}" -eq 0 ]; then
  echo "client never saw UNAVAILABLE: Kafka outage was not observed by the API" >&2
  exit 1
fi

drain_pipeline

step "database state"
psql_q "SELECT count(*) AS raid_hits, count(DISTINCT hit_id) AS distinct_hits FROM raid_hits"

assert_running api hits-consumer
step "raidctl verify"
"$BIN_DIR/raidctl$EXE" verify --redis=false

step "DONE: UNAVAILABLE during the outage, recovery afterwards, invariants hold (report: ${REPORT_JSON})"
