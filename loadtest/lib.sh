#!/usr/bin/env bash
# Общие функции сценариев нагрузки и хаоса. Подключается через `source "$(dirname "$0")/lib.sh"`
# после того, как сценарий задал свои параметры (CLANS, PLAYERS, HP, DURATION, ...).
#
# Что делает при подключении: переходит в корень репозитория, читает deploy/.env, задаёт DC,
# DATABASE_URL, REDIS_URL, BIN_DIR и ловушку EXIT (удаляет BIN_DIR и вызывает $ON_EXIT, если задан).
# Скрипт-вызыватель должен выставить `set -euo pipefail` сам.

# Git Bash на Windows иначе превращает /opt/kafka/... в путь внутри Git.
export MSYS_NO_PATHCONV=1

cd "$(dirname "${BASH_SOURCE[0]}")/.."

set -a
# shellcheck disable=SC1091
source deploy/.env
set +a

DC="docker compose -f deploy/docker-compose.yml --env-file deploy/.env"
export DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
export REDIS_URL="redis://localhost:${REDIS_PORT}"
KAFKA_BIN=/opt/kafka/bin
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-120}

BIN_DIR=$(mktemp -d)
# go.exe на Windows не понимает /tmp/... из Git Bash.
if command -v cygpath >/dev/null; then BIN_DIR=$(cygpath -m "$BIN_DIR"); fi
EXE=""
case "${OSTYPE:-}" in msys* | cygwin*) EXE=.exe ;; esac

cleanup() {
  eval "${ON_EXIT:-:}" || true
  rm -rf "$BIN_DIR"
}
trap cleanup EXIT

step() { printf '\n==> %s\n' "$*"; }
psql_q() { $DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c "$1"; }

# Суммарный лаг consumer group по всем партициям; -1, если группа ещё не видна.
group_lag() {
  docker exec kafka $KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "$1" 2>/dev/null |
    awk 'NR > 1 && $6 ~ /^[0-9]+$/ { sum += $6; seen = 1 } END { if (seen) print sum; else print -1 }'
}

# Сумма end-offset'ов всех партиций топиков: сколько записей в них записано.
topic_records() {
  local total=0 topic
  for topic in "$@"; do
    total=$((total + $(docker exec kafka $KAFKA_BIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
      --topic "$topic" --time latest | awk -F: '{ sum += $3 } END { print sum + 0 }')))
  done
  echo "$total"
}

# wait_until <описание> <команда, печатающая 0 когда готово>
wait_until() {
  local what=$1 cmd=$2 deadline=$((SECONDS + DRAIN_TIMEOUT)) value
  while :; do
    value=$(eval "$cmd")
    echo "${what}=${value}"
    [ "$value" = "0" ] && return 0
    if [ $SECONDS -ge "$deadline" ]; then
      echo "${what} did not reach 0 in ${DRAIN_TIMEOUT}s" >&2
      return 1
    fi
    sleep 2
  done
}

wait_port() { # wait_port <порт> [секунд]
  local port=$1 tries=${2:-60}
  for _ in $(seq 1 "$tries"); do
    if (echo >"/dev/tcp/localhost/${port}") 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "port ${port} did not open in ${tries}s" >&2
  return 1
}

build_tools() {
  step "build raidctl and botsim"
  go build -o "$BIN_DIR/raidctl$EXE" ./cmd/raidctl
  go build -o "$BIN_DIR/botsim$EXE" ./cmd/botsim
}

infra_up() {
  step "infrastructure: postgres, kafka, redis, topics"
  $DC up -d --wait pg kafka redis
  $DC up --no-deps kafka-init
}

# migrate_seed <кланов> <игроков на клан>: сидит только если игроков меньше, чем нужно.
migrate_seed() {
  step "migrations and seed"
  "$BIN_DIR/raidctl$EXE" migrate --dir migrations
  local players
  players=$(psql_q 'SELECT count(*) FROM players')
  if [ "$players" -lt $(($1 * $2)) ]; then
    "$BIN_DIR/raidctl$EXE" seed --clans "$1" --players "$2"
  fi
}

# start_services <сервис>...: собирает и запускает сервисы приложения, ждёт порт api.
start_services() {
  step "start: $*"
  $DC up -d --build --no-deps "$@"
  wait_port "$APP_PORT"
}

# assert_running <контейнер>...: после хаоса сервис не должен остаться лежать.
assert_running() {
  local name state
  for name in "$@"; do
    state=$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo missing)
    if [ "$state" != "running" ]; then
      echo "container ${name} is ${state} after the scenario, expected running" >&2
      return 1
    fi
  done
}

# start_load <имя сценария>: запускает botsim в фоне. Параметры из окружения (значения по умолчанию
# ниже): MODE=open|closed, TARGET_RPS (open), RATE (closed), CONNS, WATCHERS.
# Текстовый отчёт — в $BIN_DIR/botsim.out, JSON — в loadtest/results/<имя>-<время>.json.
start_load() {
  mkdir -p loadtest/results
  REPORT_JSON="loadtest/results/$1-$(date +%Y%m%d-%H%M%S).json"
  step "load (${MODE:-open}): ${CLANS} clans x ${PLAYERS} players, hp=${HP}, ${DURATION}, report ${REPORT_JSON}"
  "$BIN_DIR/botsim$EXE" --addr "localhost:${APP_PORT}" --clans "$CLANS" --players "$PLAYERS" \
    --hp "$HP" --duration "$DURATION" --mode "${MODE:-open}" --target-rps "${TARGET_RPS:-300}" \
    --rate "${RATE:-2}" --conns "${CONNS:-1}" --watchers "${WATCHERS:-0}" \
    --ramp-up "${RAMP_UP:-0s}" --json "$REPORT_JSON" \
    >"$BIN_DIR/botsim.out" &
  BOTSIM_PID=$!
}

finish_load() {
  wait "$BOTSIM_PID"
  cat "$BIN_DIR/botsim.out"
}

# drain_pipeline ждёт, пока консьюмер, relay и worker разберут все очереди.
drain_pipeline() {
  step "waiting for consumers and relays to drain"
  wait_until "hits-consumer lag" 'group_lag hits-consumer'
  wait_until "rewards-worker lag" 'group_lag rewards-worker'
  wait_until "unsent outbox records" "psql_q 'SELECT count(*) FROM outbox WHERE sent_at IS NULL'"
  wait_until "rewards-worker lag after relays" 'group_lag rewards-worker'
  wait_until "unsent outbox records after worker" "psql_q 'SELECT count(*) FROM outbox WHERE sent_at IS NULL'"
}

# report_count <код gRPC, например Unavailable>: сколько ответов с таким кодом в JSON-отчёте botsim.
report_count() {
  grep -E "\"$1\":" "$REPORT_JSON" | head -n 1 | tr -dc '0-9' || true
}

# start_lag_sampler / stop_lag_sampler: раз в секунду замеряют лаг hits-consumer, максимум — в $BIN_DIR/lag.max.
start_lag_sampler() {
  echo 0 >"$BIN_DIR/lag.max"
  (
    while :; do
      lag=$(group_lag hits-consumer)
      if [ "$lag" -gt "$(cat "$BIN_DIR/lag.max")" ]; then echo "$lag" >"$BIN_DIR/lag.max"; fi
      sleep 1
    done
  ) &
  LAG_PID=$!
}

stop_lag_sampler() {
  kill "$LAG_PID" 2>/dev/null || true
  wait "$LAG_PID" 2>/dev/null || true
}

# json_num <ключ>: число из JSON-отчёта botsim (первое вхождение ключа).
json_num() {
  grep -m1 "\"$1\":" "$REPORT_JSON" | sed 's/.*: *\([0-9.eE+-]*\),\{0,1\}$/\1/'
}

# result_row <метка>: строка таблицы результатов (markdown) по последнему отчёту botsim и лагу.
# Столбцы: сценарий | RPS | p50 | p95 | p99 | OK | Unavailable | прочие ошибки | макс. лаг консьюмера.
result_row() {
  local ok unavail total others
  ok=$(json_num ok_responses)
  unavail=$(report_count Unavailable)
  total=$(json_num rpc_total)
  others=$((total - ok - ${unavail:-0}))
  printf '| %s | %.0f | %.1f | %.1f | %.1f | %s | %s | %s | %s |\n' "$1" "$(json_num rps)" \
    "$(json_num p50)" "$(json_num p95)" "$(json_num p99)" "$ok" "${unavail:-0}" "$others" \
    "$(cat "$BIN_DIR/lag.max" 2>/dev/null || echo -)" | tee -a loadtest/results/table.md
}
