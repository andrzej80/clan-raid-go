#!/usr/bin/env bash
# Хаос этапа 11 (B6): убить один из трёх брокеров Kafka посреди нагрузки.
#
# Кластер из deploy/docker-compose.kafka-ha.yml: RF=3, min.insync.replicas=2, acks=all.
# Ожидается: запись не останавливается (лидеры партиций переезжают на живые брокеры, доля
# UNAVAILABLE — на время выборов лидера), ни один удар с успешным ответом не потерян
# (raid_hits >= успешных ответов botsim), после возвращения брокера реплики догоняют лидеров,
# raidctl verify проходит.
#
# Убивается kafka-2, а не kafka: через контейнер kafka скрипты читают лаг групп.
#
#   loadtest/chaos_broker.sh
#   BROKER_DOWNTIME=60 KILL_AFTER=10 DURATION=100s loadtest/chaos_broker.sh
set -euo pipefail

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
HP=${HP:-3000000}          # боссы живут всю нагрузку: иначе удары после гибели не попадут в raid_hits
DURATION=${DURATION:-70s}  # больше KILL_AFTER + BROKER_DOWNTIME, чтобы проверить и восстановление
KILL_AFTER=${KILL_AFTER:-5}
BROKER_DOWNTIME=${BROKER_DOWNTIME:-30}
VICTIM=${VICTIM:-kafka-2}
# Без намеренных повторов с тем же ключом: иначе успешных ответов больше, чем уникальных ударов,
# и сравнение с raid_hits ничего не доказывает.
DUP_PROB=0

COMPOSE_EXTRA="-f deploy/docker-compose.kafka-ha.yml"
KAFKA_BROKERS_EXTRA="kafka-2 kafka-3"

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"
ON_EXIT="docker start ${VICTIM} >/dev/null 2>&1"
# Для raidctl topics diff с хоста: внешний листенер брокеров и ожидаемый фактор репликации.
export KAFKA_BROKERS="localhost:${KAFKA_EXTERNAL_PORT}"
export KAFKA_REPLICATION_FACTOR=3

under_replicated() {
  docker exec kafka $KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --under-replicated-partitions 2>/dev/null | grep -c 'Partition:' || true
}

build_tools
infra_up
step "topics: replication factor and min.insync.replicas"
"$BIN_DIR/raidctl$EXE" topics diff || {
  echo "topics differ from the spec: was the cluster started without the HA overlay? run 'down' first" >&2
  exit 1
}
docker exec kafka $KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raid.hits | head -n 3
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer rewards-worker scheduler outbox-relay

# botsim переиспользует активный рейд клана: рейд прошлого прогона может умереть или истечь посреди
# нагрузки, и удары после этого законно станут опоздавшими — сравнение с ответами сломается.
step "close active botsim raids from previous runs"
psql_q "UPDATE raids SET status = 'expired', finished_at = now() WHERE status = 'active' AND boss_name LIKE 'boss-%'" >/dev/null

start_load chaos-broker
sleep "$KILL_AFTER"
step "CHAOS: kill ${VICTIM} for ${BROKER_DOWNTIME}s"
docker kill "$VICTIM" >/dev/null
sleep 5
echo "under-replicated partitions with ${VICTIM} down: $(under_replicated)"
sleep $((BROKER_DOWNTIME - 5))
docker start "$VICTIM" >/dev/null
$DC up -d --wait "$VICTIM"
finish_load

ok=$(json_num ok_responses)
unavailable=$(report_count Unavailable)
total=$(json_num rpc_total)
echo "responses: ok=${ok} unavailable=${unavailable:-0} total=${total}"

drain_pipeline
wait_until "under-replicated partitions" 'under_replicated'

step "database state"
# Только рейды этого прогона: в базе могут быть удары прошлых сценариев.
raids=$(sed -n '/^raids:/,$p' "$BIN_DIR/botsim.out" | grep -oE '[0-9a-f-]{36}' | sed "s/.*/'&'/" | paste -sd, -)
hits=$(psql_q "SELECT count(*) FROM raid_hits WHERE raid_id IN (${raids})")
psql_q "SELECT count(*) AS raid_hits, count(DISTINCT hit_id) AS distinct_hits FROM raid_hits WHERE raid_id IN (${raids})"
# Ответ «принято» дан только после подтверждения min.insync.replicas реплик, поэтому каждый такой
# удар обязан дойти до базы. Больше — нормально: запрос мог записаться, а ответ до клиента не дойти.
if [ "$hits" -lt "$ok" ]; then
  echo "LOST HITS: ${ok} accepted by the API, only ${hits} in raid_hits" >&2
  exit 1
fi

# outbox-relay без container_name (масштабируется), поэтому по имени его не проверить.
assert_running api hits-consumer rewards-worker
step "raidctl verify"
"$BIN_DIR/raidctl$EXE" verify --redis=false

step "DONE: broker loss survived, ${hits} hits in DB for ${ok} accepted, invariants hold (report: ${REPORT_JSON})"
