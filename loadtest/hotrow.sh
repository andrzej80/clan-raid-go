#!/usr/bin/env bash
# Этап 9: эксперимент «горячая строка». Одна и та же нагрузка (botsim --mode open) против трёх режимов
# приёма ударов: kafka (очередь), sync (UPDATE raids + INSERT raid_hits в запросе) и sync-full
# (вся логика консьюмера на каждый удар в запросе). Две формы нагрузки:
#   hot    — 1 клан × HOT_PLAYERS игроков: все удары в одну строку raids;
#   spread — SPREAD_CLANS кланов × SPREAD_PLAYERS: те же RPS, но по многим строкам.
# Игроков много не случайно: rate limit 5 ударов/с на игрока ограничил бы один клан из 50 игроков
# до ~250 RPS, и строка не успела бы стать узким местом.
#
# Два блока замеров:
#   1. лесенка RPS для каждого режима и формы при пуле api = POOL (по умолчанию 16);
#   2. перебор размера пула api (POOLS) на фиксированном RPS для sync-режимов: отделяет «упёрлись в
#      блокировку строки» (рост пула не помогает) от «упёрлись в пул» (помогает).
# Для каждого прогона сохраняются: отчёт botsim, ряд из pg_stat_activity/pg_locks (раз в ~секунду),
# метрики пула api из VictoriaMetrics и лаг консьюмера. Графики и таблицу делает
# loadtest/hotrow_report.py (нужен matplotlib).
#
#   loadtest/hotrow.sh
#   MODES="sync" SHAPES="hot" LADDER="500 1000" POOLS="" DURATION=20s loadtest/hotrow.sh
set -euo pipefail

MODES=${MODES:-"kafka sync sync-full"}
SHAPES=${SHAPES:-"hot spread"}
HOT_PLAYERS=${HOT_PLAYERS:-2500}
SPREAD_CLANS=${SPREAD_CLANS:-40}
SPREAD_PLAYERS=${SPREAD_PLAYERS:-60}
LADDER=${LADDER:-"250 500 1000 2000 4000"}
POOL=${POOL:-16}
FINE_LADDER=${FINE_LADDER-"100 150 200 300 400 600"}   # мелкая лесенка у границы sync-режимов; пусто = пропустить
FINE_MODES=${FINE_MODES:-"sync sync-full"}
FINE_SHAPES=${FINE_SHAPES:-"hot spread"}
POOLS=${POOLS-"4 8 16 32 64"}          # пустая строка отключает перебор пула
POOL_SWEEP_RPS=${POOL_SWEEP_RPS:-1000}
POOL_SWEEP_MODES=${POOL_SWEEP_MODES:-"sync sync-full"}
HP=${HP:-1000000000}                    # боссы не должны умирать за время замеров
DURATION=${DURATION:-30s}
RAMP_UP=${RAMP_UP:-5s}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-300}
OUT=${OUT:-loadtest/results/hotrow}

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"

export API_METRIC_INTERVAL_MS=2000      # метрики пула api раз в 2 с, а не раз в 10

# set_shape <hot|spread>: выставляет CLANS и PLAYERS.
set_shape() {
  case "$1" in
    hot) CLANS=1; PLAYERS=$HOT_PLAYERS ;;
    spread) CLANS=$SPREAD_CLANS; PLAYERS=$SPREAD_PLAYERS ;;
    *) echo "unknown shape $1" >&2; return 1 ;;
  esac
}

# reset_data <кланов> <игроков>: чистое состояние для прогона. Рейды, удары и награды удаляются,
# кланы и игроки пересоздаются под форму нагрузки, Redis очищается.
reset_data() {
  psql_q 'TRUNCATE raid_hits, raid_damage, rewards, outbox, raids, players, clans RESTART IDENTITY CASCADE' >/dev/null
  $DC exec -T redis redis-cli FLUSHALL >/dev/null
  "$BIN_DIR/raidctl$EXE" seed --clans "$1" --players "$2" >/dev/null
}

# use_api <режим> <размер пула> <форма>: пересоздаёт api с нужными ATTACK_MODE и PG_MAX_CONNS.
# Форма входит в ключ: после смены формы кланы и игроки пересозданы с теми же id, а api кэширует игроков
# на минуту (playerCacheTTL), и старый кэш отдавал бы чужие кланы (PermissionDenied).
# В sync-режимах консьюмер и остальные обработчики очереди остановлены: из Kafka им нечего читать,
# а CPU машины общий.
CURRENT_API=""
use_api() {
  [ "$CURRENT_API" = "$1/$2/$3" ] && return 0
  export ATTACK_MODE=$1 API_PG_MAX_CONNS=$2
  step "api: ATTACK_MODE=$1 PG_MAX_CONNS=$2"
  if [ "$1" = kafka ]; then
    $DC up -d --no-deps hits-consumer rewards-worker outbox-relay scheduler
  else
    $DC stop hits-consumer rewards-worker outbox-relay >/dev/null
    $DC up -d --no-deps scheduler
  fi
  $DC up -d --build --force-recreate --no-deps api
  wait_port "$APP_PORT"
  sleep 3
  CURRENT_API="$1/$2/$3"
}

# Один запрос к pg_stat_activity/pg_locks: unix-время, active, ждут блокировку, idle in transaction,
# неполученные блокировки (pg_locks), всего клиентских соединений.
PG_SAMPLE_SQL="SELECT extract(epoch from clock_timestamp())::bigint,
  count(*) FILTER (WHERE state = 'active'),
  count(*) FILTER (WHERE wait_event_type = 'Lock'),
  count(*) FILTER (WHERE state = 'idle in transaction'),
  (SELECT count(*) FROM pg_locks WHERE NOT granted),
  count(*)
FROM pg_stat_activity
WHERE datname = current_database() AND backend_type = 'client backend' AND pid <> pg_backend_pid()"

# Одна постоянная сессия psql с \watch: отдельный `docker exec` на каждый замер заметно грузил машину
# и искажал результаты. Сессия сама завершается после c замеров, с запасом на длительность прогона.
start_pg_sampler() { # start_pg_sampler <файл csv>
  local count=$(( ${DURATION%s} + 20 ))
  echo 'ts,active,lock_wait,idle_in_tx,locks_not_granted,connections' >"$1"
  local sql=${PG_SAMPLE_SQL//$'\n'/ }
  printf '%s \\watch i=1 c=%s\n' "$sql" "$count" |
    $DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -F, >>"$1" &
  PG_SAMPLER_PID=$!
}

stop_pg_sampler() {
  kill "$PG_SAMPLER_PID" 2>/dev/null || true
  wait "$PG_SAMPLER_PID" 2>/dev/null || true
}

# save_pool_metrics <файл json> <начало> <конец>: метрики пула api из VictoriaMetrics за время прогона.
save_pool_metrics() {
  local q name
  : >"$1"
  for name in acquired:'pgxpool_acquired_connections{job="api"}' \
    max:'pgxpool_max_connections{job="api"}' \
    empty_acquires:'rate(pgxpool_empty_acquire_total{job="api"}[10s])' \
    acquire_wait_s:'rate(pgxpool_empty_acquire_wait_time_nanoseconds_total{job="api"}[10s])/1e9'; do
    q=${name#*:}
    printf '%s\t' "${name%%:*}" >>"$1"
    curl -sG 'http://localhost:8428/api/v1/query_range' --data-urlencode "query=$q" \
      --data-urlencode "start=$2" --data-urlencode "end=$3" --data-urlencode 'step=2' >>"$1" || true
    echo >>"$1"
  done
}

# run_case <режим> <форма> <пул> <RPS> <блок>: один прогон. Результаты — в $OUT/<имя>/.
run_case() {
  local mode=$1 shape=$2 pool=$3 rps=$4 block=$5 name dir t0 t1
  name="${block}-${mode}-${shape}-pool${pool}-rps${rps}"
  dir="$OUT/$name"
  mkdir -p "$dir"
  step "case $name"
  set_shape "$shape"
  use_api "$mode" "$pool" "$shape"
  reset_data "$CLANS" "$PLAYERS"

  TARGET_RPS=$rps
  CONNS=1
  WATCHERS=0
  t0=$(date +%s)
  start_pg_sampler "$dir/pg.csv"
  start_load "hotrow-$name"
  finish_load >"$dir/botsim.txt" || true
  t1=$(date +%s)
  stop_pg_sampler
  if [ "$mode" = kafka ]; then
    # Лаг в момент окончания нагрузки. Раз в секунду его не снимаем: kafka-consumer-groups запускает
    # JVM, и такой сэмплер сам грузил бы машину и искажал задержки именно в режиме kafka.
    group_lag hits-consumer >"$dir/lag.max"
    drain_pipeline >/dev/null || echo "queues did not drain in time" >&2
  fi
  cp "$REPORT_JSON" "$dir/botsim.json"
  sleep 4 # последний экспорт метрик пула
  save_pool_metrics "$dir/pool.tsv" "$t0" "$((t1 + 4))"
  printf '{"name":"%s","block":"%s","mode":"%s","shape":"%s","pool":%s,"target_rps":%s,"clans":%s,"players":%s}\n' \
    "$name" "$block" "$mode" "$shape" "$pool" "$rps" "$CLANS" "$PLAYERS" >"$dir/meta.json"

  # Инварианты: sync не ведёт raid_damage, поэтому инвариант 4 для него не проверяется.
  local skip=""
  [ "$mode" = sync ] && skip="--skip-damage-aggregate"
  "$BIN_DIR/raidctl$EXE" verify --redis=false $skip | tee "$dir/verify.txt"
}

build_tools
infra_up
step "metrics: otel-collector and victoriametrics (pool metrics of api)"
$DC up -d victoriametrics otel-collector
$DC up -d --no-deps rewards-worker scheduler outbox-relay hits-consumer 2>/dev/null || true
"$BIN_DIR/raidctl$EXE" migrate --dir migrations
mkdir -p "$OUT"

for mode in $MODES; do
  for shape in $SHAPES; do
    for rps in $LADDER; do
      run_case "$mode" "$shape" "$POOL" "$rps" ladder
    done
  done
done

for mode in $FINE_MODES; do
  [ -n "$FINE_LADDER" ] || break
  for shape in $FINE_SHAPES; do
    for rps in $FINE_LADDER; do
      run_case "$mode" "$shape" "$POOL" "$rps" fine
    done
  done
done

for mode in $POOL_SWEEP_MODES; do
  [ -n "$POOLS" ] || break
  for shape in $SHAPES; do
    for pool in $POOLS; do
      run_case "$mode" "$shape" "$pool" "$POOL_SWEEP_RPS" poolsweep
    done
  done
done

# Вернуть api в исходный режим.
export ATTACK_MODE=kafka API_PG_MAX_CONNS=5
CURRENT_API=""
use_api kafka 5 restore

step "DONE: ${OUT}; build the report with: python loadtest/hotrow_report.py ${OUT}"
