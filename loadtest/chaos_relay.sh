#!/usr/bin/env bash
# Хаос этапа 8: убить все outbox-relay посреди нагрузки.
#
# Боссы умирают, пока relay лежат: события RaidEvent копятся в outbox. После перезапуска relay
# обязаны вычистить outbox, rewards-worker — выдать награды, а raidctl verify --require-rewards
# (инвариант 5) — пройти. Дубли в Kafka допустимы (at-least-once), потери — нет.
#
#   loadtest/chaos_relay.sh
#   RELAYS=3 KILL_AFTER=4 DOWNTIME=15 loadtest/chaos_relay.sh
set -euo pipefail

CLANS=${CLANS:-20}          # outbox получает одну запись на убитого босса, поэтому кланов много
PLAYERS=${PLAYERS:-50}
HP=${HP:-400000}         # при 20 кланах и 600 RPS босс умирает на ~20-й секунде, внутри окна простоя
DURATION=${DURATION:-60s}
RELAYS=${RELAYS:-2}
KILL_AFTER=${KILL_AFTER:-4}
DOWNTIME=${DOWNTIME:-25}      # окно простоя: от KILL_AFTER до KILL_AFTER+DOWNTIME секунд

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"
ON_EXIT='$DC up -d --no-deps --scale "outbox-relay=${RELAYS}" outbox-relay >/dev/null 2>&1'

build_tools
infra_up
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer rewards-worker scheduler
$DC up -d --build --no-deps --scale "outbox-relay=${RELAYS}" outbox-relay

TARGET_RPS=${TARGET_RPS:-600}
start_load chaos-relay
sleep "$KILL_AFTER"
step "CHAOS: kill all outbox-relay for ${DOWNTIME}s"
$DC kill outbox-relay
sleep "$DOWNTIME"
unsent=$(psql_q 'SELECT count(*) FROM outbox WHERE sent_at IS NULL')
echo "unsent outbox records while relays were down: ${unsent}"
[ "$unsent" -ge "$CLANS" ] || echo "WARNING: only ${unsent} of ${CLANS} bosses died while relays were down, the window missed some deaths" >&2
$DC up -d --no-deps --scale "outbox-relay=${RELAYS}" outbox-relay
finish_load

drain_pipeline

step "outbox records vs records in Kafka (duplicates allowed, losses are not)"
in_outbox=$(psql_q "SELECT count(*) FROM outbox WHERE sent_at IS NOT NULL")
in_kafka=$(topic_records raid.events rewards.granted)
echo "outbox sent=${in_outbox} kafka=${in_kafka}"
[ "$in_kafka" -ge "$in_outbox" ] || {
  echo "Kafka has fewer records than the outbox sent: records were lost" >&2
  exit 1
}

assert_running api hits-consumer rewards-worker
step "raidctl verify --require-rewards"
"$BIN_DIR/raidctl$EXE" verify --redis=false --require-rewards

step "DONE: no records lost after killing the relays, invariant 5 holds (report: ${REPORT_JSON})"
