#!/usr/bin/env bash
# Сценарий этапа 4: нагрузка botsim, остановка нагрузки, потеря всего Redis (FLUSHALL),
# raidctl rebuild-cache и проверка инварианта 6 (лидерборд в Redis совпадает с raid_damage).
#
# Требования: docker compose, Go, поднятые pg, kafka и redis из deploy/docker-compose.yml.
# Скрипт сбрасывает ВЕСЬ Redis из compose (там только кэш и rate limit, источник правды — Postgres).
#
#   loadtest/redis_rebuild.sh
#   HP=3000000 DURATION=30s loadtest/redis_rebuild.sh
set -euo pipefail

export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
RATE=${RATE:-2}
HP=${HP:-1500000}
DURATION=${DURATION:-20s}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-90}

set -a
# shellcheck disable=SC1091
source deploy/.env
set +a

DC="docker compose -f deploy/docker-compose.yml --env-file deploy/.env"
export DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
export REDIS_URL="redis://localhost:${REDIS_PORT}"
KAFKA_BIN=/opt/kafka/bin

BIN_DIR=$(mktemp -d)
if command -v cygpath >/dev/null; then BIN_DIR=$(cygpath -m "$BIN_DIR"); fi
EXE=""
case "${OSTYPE:-}" in msys* | cygwin*) EXE=.exe ;; esac
trap 'rm -rf "$BIN_DIR"' EXIT

step() { printf '\n==> %s\n' "$*"; }

step "build raidctl and botsim"
go build -o "$BIN_DIR/raidctl$EXE" ./cmd/raidctl
go build -o "$BIN_DIR/botsim$EXE" ./cmd/botsim

step "infrastructure: postgres, kafka, redis, topics"
$DC up -d --wait pg kafka redis
$DC up --build --no-deps kafka-init

step "migrations and seed"
"$BIN_DIR/raidctl$EXE" migrate --dir migrations
players=$($DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c 'SELECT count(*) FROM players')
if [ "$players" -lt $((CLANS * PLAYERS)) ]; then
  "$BIN_DIR/raidctl$EXE" seed --clans "$CLANS" --players "$PLAYERS"
fi

step "rebuild-cache for data written before Redis was in use (what you run once when deploying stage 4)"
"$BIN_DIR/raidctl$EXE" rebuild-cache

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
"$BIN_DIR/botsim$EXE" --addr "localhost:${APP_PORT}" --clans "$CLANS" --players "$PLAYERS" \
  --rate "$RATE" --hp "$HP" --duration "$DURATION"

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
sleep 2 # запись в Redis идёт сразу после коммита пачки; даём последней пачке завершиться

step "invariants 1-4, 6, 7 on live data (Redis was written by the consumer)"
"$BIN_DIR/raidctl$EXE" verify

step "FLUSHALL: Redis loses everything"
$DC exec -T redis redis-cli FLUSHALL
if "$BIN_DIR/raidctl$EXE" verify; then
  echo "verify must fail after FLUSHALL" >&2
  exit 1
fi

step "GetLeaderboard falls back to PostgreSQL while Redis is empty"
raid_id=$($DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c \
  "SELECT r.id FROM raids r JOIN raid_damage d ON d.raid_id = r.id GROUP BY r.id ORDER BY max(d.total_damage) DESC LIMIT 1")
if command -v grpcurl >/dev/null; then
  grpcurl -plaintext -H 'x-player-id: 1' -d "{\"raid_id\": \"${raid_id}\", \"limit\": 3}" \
    "localhost:${APP_PORT}" raidboss.v1.RaidService/GetLeaderboard
else
  echo "grpcurl not found, skipping (needs GRPC_REFLECTION=true)"
fi

step "raidctl rebuild-cache"
"$BIN_DIR/raidctl$EXE" rebuild-cache

step "raidctl verify"
"$BIN_DIR/raidctl$EXE" verify

step "DONE: invariant 6 holds after FLUSHALL and rebuild-cache"
