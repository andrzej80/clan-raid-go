#!/usr/bin/env bash
# Хаос этапа 8: перезапустить Redis посреди нагрузки.
#
# Redis — кэш, лидерборд и rate limit, источник правды — Postgres. Ожидается: api и консьюмер
# переживают перезапуск, инварианты 1–5 и 7 выполняются сразу, а инвариант 6 (лидерборд) — после
# raidctl rebuild-cache.
#
#   loadtest/chaos_redis.sh
#   KILL_AFTER=8 loadtest/chaos_redis.sh
set -euo pipefail

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
HP=${HP:-3000000}         # боссы живут всю нагрузку
DURATION=${DURATION:-30s}
KILL_AFTER=${KILL_AFTER:-8}

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"
ON_EXIT='$DC up -d --no-deps redis >/dev/null 2>&1'

build_tools
infra_up
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer rewards-worker scheduler outbox-relay

start_load chaos-redis
sleep "$KILL_AFTER"
step "CHAOS: restart redis (all keys are lost, there is no volume)"
$DC restart redis
finish_load

drain_pipeline
sleep 2 # запись в Redis идёт сразу после коммита пачки

assert_running api hits-consumer
step "invariants without Redis (Postgres is the source of truth)"
"$BIN_DIR/raidctl$EXE" verify --redis=false

step "raidctl rebuild-cache and full verify (invariant 6)"
"$BIN_DIR/raidctl$EXE" rebuild-cache
"$BIN_DIR/raidctl$EXE" verify

step "DONE: services survived the Redis restart, leaderboard restored from Postgres (report: ${REPORT_JSON})"
