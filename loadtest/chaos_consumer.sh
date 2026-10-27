#!/usr/bin/env bash
# Сценарий этапа 3 (и хаос этапа 8): нагрузка botsim, docker kill консьюмера посреди нагрузки,
# перезапуск, проверка инвариантов (raidctl verify) и DLQ для битого сообщения.
#
# Требования: docker compose, Go, поднятые pg и kafka из deploy/docker-compose.yml.
# Скрипт ничего не удаляет: данные добавляются в текущую БД (кланы и игроков создаёт только если их нет).
#
#   loadtest/chaos_consumer.sh
#   HP=3000000 DURATION=60s KILL_AFTER=10 DOWNTIME=5 loadtest/chaos_consumer.sh
#   MODE=open TARGET_RPS=500 loadtest/chaos_consumer.sh
set -euo pipefail

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
MODE=${MODE:-closed}      # closed — как раньше; open — нагрузка по расписанию (этап 8)
RATE=${RATE:-2}
HP=${HP:-1500000}
DURATION=${DURATION:-40s}
KILL_AFTER=${KILL_AFTER:-8}   # через сколько секунд после старта нагрузки убить консьюмер
DOWNTIME=${DOWNTIME:-5}       # сколько секунд консьюмер лежит
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-90}

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"
ON_EXIT='docker start hits-consumer >/dev/null 2>&1'

build_tools
infra_up
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer

start_load chaos-consumer
sleep "$KILL_AFTER"
step "docker kill hits-consumer (lag before: $(group_lag hits-consumer))"
docker kill hits-consumer >/dev/null
sleep "$DOWNTIME"
step "docker start hits-consumer"
docker start hits-consumer >/dev/null
finish_load

step "waiting for the consumer to drain the topic"
wait_until "hits-consumer lag" 'group_lag hits-consumer'

step "broken message -> DLQ"
dlq_end() {
  docker exec kafka $KAFKA_BIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic raid.hits.dlq --time -1 | awk -F: '{ sum += $3 } END { print sum + 0 }'
}
before=$(dlq_end)
printf 'this-is-not-protobuf\n' | docker exec -i kafka $KAFKA_BIN/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic raid.hits >/dev/null
sleep 5
after=$(dlq_end)
if [ "$after" -le "$before" ]; then
  echo "broken message did not reach raid.hits.dlq (offsets ${before} -> ${after})" >&2
  exit 1
fi
echo "dlq offsets: ${before} -> ${after}"

step "database state"
$DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT status, count(*) AS raids FROM raids GROUP BY status ORDER BY status" -c \
  "SELECT count(*) AS raid_hits, count(DISTINCT hit_id) AS distinct_hits FROM raid_hits" -c \
  "SELECT count(*) AS defeated_events FROM outbox WHERE topic = 'raid.events'"

step "raidctl verify"
# Redis здесь не проверяется: после kill консьюмера он может отставать от Postgres (см. loadtest/redis_rebuild.sh).
"$BIN_DIR/raidctl$EXE" verify --redis=false

step "DONE: invariants hold after kill/restart of the consumer (report: ${REPORT_JSON})"
