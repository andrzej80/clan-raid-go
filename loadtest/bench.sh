#!/usr/bin/env bash
# Замеры этапа 8: лесенка RPS (open model), одно соединение против пула, подписчики WatchRaid.
# Каждый прогон: botsim --mode open, лаг консьюмера снимается раз в секунду, после прогона ждём,
# пока очереди разберутся, и проверяем инварианты. Строки таблицы пишутся в loadtest/results/table.md
# (каталог в .gitignore; итоговую таблицу переносят в README вручную).
#
#   loadtest/bench.sh
#   LADDER="500 1000 2000" CONN_RPS=2000 DURATION=30s loadtest/bench.sh
set -euo pipefail

CLANS=${CLANS:-40}
PLAYERS=${PLAYERS:-50}
HP=${HP:-1000000000}   # боссы не должны умирать за время замеров
DURATION=${DURATION:-40s}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-300}
RAMP_UP=${RAMP_UP:-10s}
LADDER=${LADDER:-"1000 2000 4000 6000 8000"}
CONN_RPS=${CONN_RPS:-6000}      # нагрузка для сравнения 1 соединения и пула
CONN_POOL=${CONN_POOL:-8}
WATCHERS_N=${WATCHERS_N:-1000}
WATCH_RPS=${WATCH_RPS:-1000}

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"

# bench_case <метка> <RPS>: один прогон с остальными параметрами из окружения (CONNS, WATCHERS).
bench_case() {
  TARGET_RPS=$2
  step "case: $1 (target ${2} rps, conns=${CONNS:-1}, watchers=${WATCHERS:-0})"
  start_lag_sampler
  start_load "bench-$1"
  finish_load >/dev/null
  stop_lag_sampler
  drain_pipeline >/dev/null
  result_row "$1"
}

build_tools
infra_up
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer rewards-worker scheduler outbox-relay
mkdir -p loadtest/results
{
  echo
  echo "# bench $(date '+%Y-%m-%d %H:%M') clans=${CLANS} players=${PLAYERS} duration=${DURATION} ramp-up=${RAMP_UP}"
  echo '| сценарий | RPS | p50, мс | p95, мс | p99, мс | OK | Unavailable | прочие | макс. лаг |'
  echo '|---|---|---|---|---|---|---|---|---|'
} | tee -a loadtest/results/table.md

CONNS=1
WATCHERS=0
for rps in $LADDER; do
  bench_case "rps-${rps}" "$rps"
done

CONNS=1
bench_case "conns-1-rps-${CONN_RPS}" "$CONN_RPS"
CONNS=$CONN_POOL
bench_case "conns-${CONN_POOL}-rps-${CONN_RPS}" "$CONN_RPS"

CONNS=1
WATCHERS=$WATCHERS_N
bench_case "watchers-${WATCHERS_N}-rps-${WATCH_RPS}" "$WATCH_RPS"

step "raidctl verify"
"$BIN_DIR/raidctl$EXE" verify --redis=false
step "DONE: results in loadtest/results/table.md"
