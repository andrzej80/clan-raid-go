#!/usr/bin/env bash
# Сценарий этапа 3: нагрузка botsim, docker kill консьюмера посреди нагрузки, перезапуск,
# проверка инвариантов (raidctl verify) и DLQ для битого сообщения.
#
# Требования: docker compose, Go, поднятые pg и kafka из deploy/docker-compose.yml.
# Скрипт ничего не удаляет: данные добавляются в текущую БД (кланы и игроков создаёт только если их нет).
#
#   loadtest/chaos_consumer.sh
#   HP=3000000 DURATION=60s KILL_AFTER=10 DOWNTIME=5 loadtest/chaos_consumer.sh
set -euo pipefail

# Git Bash на Windows иначе превращает /opt/kafka/... в путь внутри Git.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
RATE=${RATE:-2}
HP=${HP:-1500000}
DURATION=${DURATION:-40s}
KILL_AFTER=${KILL_AFTER:-8}   # через сколько секунд после старта нагрузки убить консьюмер
DOWNTIME=${DOWNTIME:-5}       # сколько секунд консьюмер лежит
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-90}

set -a
# shellcheck disable=SC1091
source deploy/.env
set +a

DC="docker compose -f deploy/docker-compose.yml --env-file deploy/.env"
export DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
KAFKA_BIN=/opt/kafka/bin

BIN_DIR=$(mktemp -d)
# go.exe на Windows не понимает /tmp/... из Git Bash.
if command -v cygpath >/dev/null; then BIN_DIR=$(cygpath -m "$BIN_DIR"); fi
EXE=""
case "${OSTYPE:-}" in msys* | cygwin*) EXE=.exe ;; esac
trap 'rm -rf "$BIN_DIR"' EXIT

step() { printf '\n==> %s\n' "$*"; }

step "build raidctl and botsim"
go build -o "$BIN_DIR/raidctl$EXE" ./cmd/raidctl
go build -o "$BIN_DIR/botsim$EXE" ./cmd/botsim

step "infrastructure: postgres, kafka, topics"
$DC up -d --wait pg kafka
$DC up --no-deps kafka-init

step "migrations and seed"
"$BIN_DIR/raidctl$EXE" migrate --dir migrations
players=$($DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c 'SELECT count(*) FROM players')
if [ "$players" -lt $((CLANS * PLAYERS)) ]; then
  "$BIN_DIR/raidctl$EXE" seed --clans "$CLANS" --players "$PLAYERS"
fi

step "start api and hits-consumer"
$DC up -d --build --no-deps api hits-consumer
for _ in $(seq 1 60); do
  if (echo >"/dev/tcp/localhost/${APP_PORT}") 2>/dev/null; then break; fi
  sleep 1
done

consumer_lag() {
  docker exec kafka $KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group hits-consumer 2>/dev/null |
    awk 'NR > 1 && $6 ~ /^[0-9]+$/ { sum += $6; seen = 1 } END { if (seen) print sum; else print -1 }'
}

step "load: ${CLANS} clans x ${PLAYERS} players, ${RATE} hits/s each, hp=${HP}, ${DURATION}"
REPORT="$BIN_DIR/botsim.out"
"$BIN_DIR/botsim$EXE" --addr "localhost:${APP_PORT}" --clans "$CLANS" --players "$PLAYERS" \
  --rate "$RATE" --hp "$HP" --duration "$DURATION" >"$REPORT" &
BOTSIM_PID=$!

sleep "$KILL_AFTER"
step "docker kill hits-consumer (lag before: $(consumer_lag))"
docker kill hits-consumer >/dev/null
sleep "$DOWNTIME"
step "docker start hits-consumer"
docker start hits-consumer >/dev/null

wait "$BOTSIM_PID"
cat "$REPORT"

step "waiting for the consumer to drain the topic"
deadline=$((SECONDS + DRAIN_TIMEOUT))
while :; do
  lag=$(consumer_lag)
  echo "lag=${lag}"
  [ "$lag" = "0" ] && break
  if [ $SECONDS -ge $deadline ]; then
    echo "lag did not reach 0 in ${DRAIN_TIMEOUT}s" >&2
    exit 1
  fi
  sleep 2
done

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
"$BIN_DIR/raidctl$EXE" verify

step "DONE: invariants hold after kill/restart of the consumer"
