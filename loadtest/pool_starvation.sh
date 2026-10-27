#!/usr/bin/env bash
# Сценарий этапа 7: голодание пула соединений hits-consumer на дашборде и в метриках.
#
# 1. гоняет botsim при штатном PG_MAX_CONNS у hits-consumer (эталон);
# 2. перезапускает hits-consumer с урезанным PG_MAX_CONNS (по умолчанию 2) и гоняет ту же нагрузку;
# 3. после каждого прогона печатает метрики пула, лага и пачек из VictoriaMetrics за окно прогона;
# 4. возвращает hits-consumer к настройкам из compose.
# Поднятый override лежит во временном файле, deploy/docker-compose.yml не меняется.
# Перед каждым прогоном активные рейды botsim (boss-N) переводятся в expired, чтобы оба прогона
# шли по свежим рейдам с полным HP.
#
# Метрики приходят раз в OTEL_METRIC_EXPORT_INTERVAL (10 с в compose), поэтому после прогона скрипт
# ждёт ещё EXPORT_WAIT секунд. Gauge (acquired_connections, лаг) снимается раз в интервал экспорта и
# может пропустить короткий пик: для голодания надёжны счётчики empty_acquire и время ожидания.
#
# Требования: docker compose, Go, curl, node; стек из deploy/ запущен (docker compose up -d --build);
# deploy/.env (см. deploy/.env.example).
#
#   loadtest/pool_starvation.sh
#   STARVED_CONNS=1 DURATION=60s loadtest/pool_starvation.sh
set -euo pipefail

export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

CLANS=${CLANS:-10}
PLAYERS=${PLAYERS:-50}
RATE=${RATE:-4}
HP=${HP:-1000000000} # боссы не должны умирать посреди прогона
DURATION=${DURATION:-40s}
STARVED_CONNS=${STARVED_CONNS:-2}
EXPORT_WAIT=${EXPORT_WAIT:-25}
VM_URL=${VM_URL:-http://localhost:8428}

set -a
# shellcheck disable=SC1091
source deploy/.env
set +a

DC="docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile dev"
export DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"

OVERRIDE=$(mktemp)
if command -v cygpath >/dev/null; then OVERRIDE_PATH=$(cygpath -m "$OVERRIDE"); else OVERRIDE_PATH=$OVERRIDE; fi
cat >"$OVERRIDE" <<EOF
services:
  hits-consumer:
    environment:
      - PG_MAX_CONNS=${STARVED_CONNS}
EOF

restore() {
  $DC up -d hits-consumer >/dev/null 2>&1 || true
  rm -f "$OVERRIDE"
}
trap restore EXIT

step() { printf '\n==> %s\n' "$*"; }

# query выводит значение первого элемента ответа instant-запроса к VictoriaMetrics или n/a.
query() {
  local label=$1 expr=$2 at=$3
  curl -s --data-urlencode "query=$expr" --data-urlencode "time=$at" "$VM_URL/api/v1/query" |
    node -e '
      const r = JSON.parse(require("fs").readFileSync(0, "utf8")).data.result;
      const v = r.length ? Number(r[0].value[1]) : NaN;
      console.log(process.argv[1].padEnd(40), Number.isNaN(v) ? "n/a" : (Math.abs(v) < 10 ? v.toFixed(3) : v.toFixed(0)));
    ' "$label"
}

# stats печатает метрики hits-consumer за окно [start, end].
stats() {
  local start=$1 end=$2
  local at=$((end + EXPORT_WAIT - 10)) w
  w="$((at - start))s"
  local j='job="hits-consumer"'
  query "pgxpool max_connections" "max(pgxpool_max_connections{$j})" "$at"
  query "acquired_connections (max, sampled)" "max(max_over_time(pgxpool_acquired_connections{$j}[$w]))" "$at"
  query "acquires total" "sum(increase(pgxpool_acquires_total{$j}[$w]))" "$at"
  query "acquires that had to wait" "sum(increase(pgxpool_empty_acquire_total{$j}[$w]))" "$at"
  query "time spent waiting, s" "sum(increase(pgxpool_empty_acquire_wait_time_nanoseconds_total{$j}[$w])) / 1e9" "$at"
  query "canceled acquires" "sum(increase(pgxpool_canceled_acquires_total{$j}[$w]))" "$at"
  query "consumer lag, max (sampled)" "max_over_time(sum(raidboss_kafka_consumer_lag)[$w:10s])" "$at"
  query "batch duration p95, s" "histogram_quantile(0.95, sum by (le) (increase(raidboss_consumer_batch_duration_seconds_bucket[$w])))" "$at"
  query "batch size p95, records" "histogram_quantile(0.95, sum by (le) (increase(raidboss_consumer_batch_size_bucket[$w])))" "$at"
}

# reset_raids закрывает активные рейды botsim (boss-N). botsim переиспользует любой активный рейд клана:
# с прошлого прогона мог остаться рейд, который истечёт или умрёт посреди замера и испортит сравнение.
reset_raids() {
  $DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -c \
    "UPDATE raids SET status = 'expired', finished_at = now() WHERE status = 'active' AND boss_name LIKE 'boss-%'" >/dev/null
}

run_load() {
  local label=$1 start end
  reset_raids
  step "$label: botsim -duration $DURATION -clans $CLANS -players $PLAYERS -rate $RATE -hp $HP"
  start=$(date +%s)
  go run ./cmd/botsim -duration "$DURATION" -clans "$CLANS" -players "$PLAYERS" -rate "$RATE" -hp "$HP" 2>&1 |
    grep -E "OK responses|retries|latency|^  [A-Za-z]+ +[0-9]+$"
  end=$(date +%s)
  printf 'waiting %s s for the last metrics export...\n' "$EXPORT_WAIT"
  sleep "$EXPORT_WAIT"
  stats "$start" "$end"
}

step "baseline: hits-consumer with the compose PG_MAX_CONNS"
$DC up -d hits-consumer >/dev/null
sleep 5
run_load "baseline"

step "starved: hits-consumer with PG_MAX_CONNS=$STARVED_CONNS"
docker compose -f deploy/docker-compose.yml -f "$OVERRIDE_PATH" --env-file deploy/.env --profile dev up -d hits-consumer >/dev/null
sleep 5
run_load "starved PG_MAX_CONNS=$STARVED_CONNS"

step "done: hits-consumer is restored to the compose settings"
