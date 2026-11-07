#!/usr/bin/env bash
# Хаос этапа 11 (A1): строка одного рейда заблокирована в Postgres, остальные рейды не должны ждать.
#
# Под нагрузкой отдельная сессия держит SELECT ... FOR UPDATE на строке одного рейда LOCK_SECONDS
# секунд. Пачка этого рейда в hits-consumer ждёт блокировку, обрывается по таймауту попытки (30 с)
# и повторяется. Ожидается:
#   - удары остальных рейдов продолжают попадать в базу, пока строка заблокирована;
#   - отстаёт не больше одной партиции (той, где заблокированный рейд), её чтение ставится на паузу;
#   - после снятия блокировки отставшая партиция догоняет, ни один принятый удар не потерян,
#     raidctl verify проходит.
#
#   loadtest/chaos_partition.sh
#   LOCK_SECONDS=90 DURATION=130s loadtest/chaos_partition.sh
set -euo pipefail

CLANS=${CLANS:-5}
PLAYERS=${PLAYERS:-50}
# Боссы живут всю нагрузку, иначе удары после гибели станут опоздавшими: ~300 RPS за 90 с дают
# по ~3 млн урона на каждый из 5 рейдов.
HP=${HP:-10000000}
DURATION=${DURATION:-90s}  # больше LOCK_AFTER + LOCK_SECONDS, чтобы проверить и догон
LOCK_AFTER=${LOCK_AFTER:-5}
LOCK_SECONDS=${LOCK_SECONDS:-60}
LAGGING_THRESHOLD=${LAGGING_THRESHOLD:-200} # лаг партиции, начиная с которого она считается отстающей
# Без намеренных повторов с тем же ключом: иначе успешных ответов больше, чем уникальных ударов.
DUP_PROB=0

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"

# Лаг каждой партиции hits-consumer, по строке «партиция лаг».
partition_lags() {
  docker exec kafka $KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group hits-consumer 2>/dev/null |
    awk 'NR > 1 && $6 ~ /^[0-9]+$/ { print $3, $6 }'
}

hits_of() { psql_q "SELECT count(*) FROM raid_hits WHERE raid_id = '$1'"; }
hits_except() { psql_q "SELECT count(*) FROM raid_hits WHERE raid_id IN (${RAIDS}) AND raid_id <> '$1'"; }

build_tools
infra_up
migrate_seed "$CLANS" "$PLAYERS"
start_services api hits-consumer rewards-worker scheduler outbox-relay

step "close active botsim raids from previous runs"
psql_q "UPDATE raids SET status = 'expired', finished_at = now() WHERE status = 'active' AND boss_name LIKE 'boss-%'" >/dev/null

start_load chaos-partition
sleep "$LOCK_AFTER"

RAIDS=$(psql_q "SELECT string_agg(quote_literal(id::text), ',') FROM raids WHERE status = 'active' AND boss_name LIKE 'boss-%'")
VICTIM=$(psql_q "SELECT id FROM raids WHERE status = 'active' AND boss_name LIKE 'boss-%' ORDER BY clan_id LIMIT 1")
echo "raids: ${RAIDS}"
echo "locked raid: ${VICTIM}"

step "CHAOS: lock the row of raid ${VICTIM} for ${LOCK_SECONDS}s"
$DC exec -T pg psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -c \
  "BEGIN; SELECT 1 FROM raids WHERE id = '${VICTIM}' FOR UPDATE; SELECT pg_sleep(${LOCK_SECONDS}); COMMIT;" >/dev/null &
LOCK_PID=$!
ON_EXIT="kill ${LOCK_PID} 2>/dev/null"
sleep 3 # блокировка взята, текущие пачки рейда упёрлись в неё

victim_before=$(hits_of "$VICTIM")
others_before=$(hits_except "$VICTIM")
sleep $((LOCK_SECONDS / 2))
victim_mid=$(hits_of "$VICTIM")
others_mid=$(hits_except "$VICTIM")
echo "during the lock: locked raid ${victim_before} -> ${victim_mid} hits, other raids ${others_before} -> ${others_mid} hits"

echo "partition lags during the lock (partition lag):"
partition_lags | tee "$BIN_DIR/lags.txt"
lagging=$(awk -v t="$LAGGING_THRESHOLD" '$2 > t' "$BIN_DIR/lags.txt" | wc -l | tr -d ' ')
echo "partitions lagging more than ${LAGGING_THRESHOLD} records: ${lagging}"
docker logs --since 2m hits-consumer 2>&1 | grep -E 'kafka partition (paused|resumed)' | tail -n 3 || true

if [ "$others_mid" -le "$others_before" ]; then
  echo "other raids stopped while one raid row was locked: ${others_before} -> ${others_mid}" >&2
  exit 1
fi
if [ "$victim_mid" -ne "$victim_before" ]; then
  echo "locked raid got hits while its row was locked: ${victim_before} -> ${victim_mid}" >&2
  exit 1
fi
if [ "$lagging" -gt 1 ]; then
  echo "${lagging} partitions lag behind, expected at most the one with the locked raid" >&2
  exit 1
fi

wait "$LOCK_PID" || true
ON_EXIT=""
finish_load
ok=$(json_num ok_responses)

drain_pipeline

step "database state"
hits=$(psql_q "SELECT count(*) FROM raid_hits WHERE raid_id IN (${RAIDS})")
echo "hits in DB: ${hits}, accepted by the API: ${ok}"
if [ "$hits" -lt "$ok" ]; then
  echo "LOST HITS: ${ok} accepted by the API, only ${hits} in raid_hits" >&2
  exit 1
fi

assert_running api hits-consumer
step "raidctl verify"
"$BIN_DIR/raidctl$EXE" verify --redis=false

step "DONE: other raids kept going while one raid was locked, ${hits} hits in DB for ${ok} accepted (report: ${REPORT_JSON})"
