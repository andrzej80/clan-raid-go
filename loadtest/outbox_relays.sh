#!/usr/bin/env bash
# Сценарий этапа 5: три outbox-relay параллельно, награды и истечение рейдов.
#
# 1. поднимает api, hits-consumer, rewards-worker, scheduler и три экземпляра outbox-relay;
# 2. заводит рейд, который истечёт через EXPIRE_IN секунд;
# 3. гоняет botsim: боссы умирают, а консьюмер, relay и worker доводят события до наград;
# 4. ждёт, пока консьюмер, relay и worker разберут очереди;
# 5. проверяет: ни одна запись outbox не опубликована дважды (число записей в Kafka = число записей
#    в outbox), рейд истёк вовремя и без наград, `raidctl verify --require-rewards` проходит.
# CHAOS=1 добавляет убийство одного relay посреди нагрузки: дубли тогда допустимы (at-least-once),
# поэтому точное сравнение отключается, а verify обязан пройти.
#
# Требования: docker compose, Go; deploy/.env (см. deploy/.env.example).
#
#   loadtest/outbox_relays.sh
#   HP=1000000 DURATION=40s CHAOS=1 loadtest/outbox_relays.sh
set -euo pipefail

export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
RATE=${RATE:-2}
HP=${HP:-500000}
DURATION=${DURATION:-30s}
RELAYS=${RELAYS:-3}
EXPIRE_IN=${EXPIRE_IN:-15}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-120}
CHAOS=${CHAOS:-0}

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
psql_q() { $DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c "$1"; }

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

wait_until() { # wait_until <описание> <команда, печатающая 0 когда готово>
  local what=$1 cmd=$2 deadline=$((SECONDS + DRAIN_TIMEOUT)) value
  while :; do
    value=$(eval "$cmd")
    echo "${what}=${value}"
    [ "$value" = "0" ] && return 0
    if [ $SECONDS -ge $deadline ]; then
      echo "${what} did not reach 0 in ${DRAIN_TIMEOUT}s" >&2
      return 1
    fi
    sleep 2
  done
}

step "build raidctl and botsim"
go build -o "$BIN_DIR/raidctl$EXE" ./cmd/raidctl
go build -o "$BIN_DIR/botsim$EXE" ./cmd/botsim

step "infrastructure: postgres, kafka, redis, topics"
$DC up -d --wait pg kafka redis
$DC up --no-deps kafka-init

step "migrations and seed"
"$BIN_DIR/raidctl$EXE" migrate --dir migrations
players=$(psql_q 'SELECT count(*) FROM players')
if [ "$players" -lt $((CLANS * PLAYERS)) ]; then
  "$BIN_DIR/raidctl$EXE" seed --clans "$CLANS" --players "$PLAYERS"
fi

step "start api, hits-consumer, rewards-worker, scheduler and ${RELAYS} outbox-relay"
$DC up -d --build --no-deps api hits-consumer rewards-worker scheduler
$DC up -d --build --no-deps --scale "outbox-relay=${RELAYS}" outbox-relay
running=$($DC ps -q outbox-relay | wc -l | tr -d ' ')
if [ "$running" != "$RELAYS" ]; then
  echo "expected ${RELAYS} outbox-relay containers, got ${running}" >&2
  exit 1
fi
for _ in $(seq 1 60); do
  if (echo >"/dev/tcp/localhost/${APP_PORT}") 2>/dev/null; then break; fi
  sleep 1
done

step "raid that expires in ${EXPIRE_IN}s (own clan, nobody attacks it)"
expiring=$(psql_q "
  WITH c AS (INSERT INTO clans (name) VALUES ('expiring-' || gen_random_uuid()) RETURNING id)
  INSERT INTO raids (id, clan_id, boss_name, hp_max, hp_left, status, started_at, ends_at)
  SELECT gen_random_uuid(), c.id, 'Sleeper', 1000, 1000, 'active', now(), now() + interval '${EXPIRE_IN} seconds'
  FROM c RETURNING id" | head -n 1)
echo "expiring raid: ${expiring}"

step "load: ${CLANS} clans x ${PLAYERS} players, ${RATE} hits/s each, hp=${HP}, ${DURATION}"
"$BIN_DIR/botsim$EXE" --addr "localhost:${APP_PORT}" --clans "$CLANS" --players "$PLAYERS" \
  --rate "$RATE" --hp "$HP" --duration "$DURATION" &
botsim_pid=$!
if [ "$CHAOS" = "1" ]; then
  sleep 8
  victim=$($DC ps -q outbox-relay | head -n 1)
  step "CHAOS: docker kill one outbox-relay (${victim:0:12}), then start it again"
  docker kill "$victim"
  sleep 3
  $DC up -d --no-deps --scale "outbox-relay=${RELAYS}" outbox-relay
fi
wait "$botsim_pid"

step "waiting for consumers and relays to drain"
wait_until "hits-consumer lag" 'group_lag hits-consumer'
wait_until "rewards-worker lag" 'group_lag rewards-worker'
wait_until "unsent outbox records" "psql_q 'SELECT count(*) FROM outbox WHERE sent_at IS NULL'"
wait_until "rewards-worker lag after relays" 'group_lag rewards-worker'
wait_until "unsent outbox records after worker" "psql_q 'SELECT count(*) FROM outbox WHERE sent_at IS NULL'"

step "the expiring raid is expired, without rewards"
wait_until "raids still active that must have expired" \
  "psql_q \"SELECT count(*) FROM raids WHERE id = '${expiring}' AND status <> 'expired'\""
rewards_for_expired=$(psql_q "SELECT count(*) FROM rewards WHERE raid_id = '${expiring}'")
if [ "$rewards_for_expired" != "0" ]; then
  echo "expired raid got ${rewards_for_expired} rewards" >&2
  exit 1
fi

step "outbox records vs records in Kafka"
in_outbox=$(psql_q "SELECT count(*) FROM outbox WHERE sent_at IS NOT NULL")
in_kafka=$(topic_records raid.events rewards.granted)
echo "outbox sent=${in_outbox} kafka=${in_kafka}"
if [ "$CHAOS" = "1" ]; then
  [ "$in_kafka" -ge "$in_outbox" ] || {
    echo "Kafka has fewer records than the outbox sent: records were lost" >&2
    exit 1
  }
  echo "chaos run: duplicates are allowed (at-least-once), difference=$((in_kafka - in_outbox))"
elif [ "$in_kafka" != "$in_outbox" ]; then
  echo "published ${in_kafka} records for ${in_outbox} outbox rows: a record was published twice or lost" >&2
  exit 1
fi

step "raidctl outbox dump (last 5 reward events)"
"$BIN_DIR/raidctl$EXE" outbox dump --topic rewards.granted --limit 5

step "raidctl verify --require-rewards"
"$BIN_DIR/raidctl$EXE" verify --redis=false --require-rewards

if [ "$CHAOS" = "1" ]; then
  step "DONE: no records lost after killing a relay, invariant 5 holds, the raid expired on time"
else
  step "DONE: ${RELAYS} relays published every record once, invariant 5 holds, the raid expired on time"
fi
