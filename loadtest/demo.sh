#!/usr/bin/env bash
# Демо этапа 10: поднимает окружение, запускает botsim и показывает raidwatch до гибели босса.
# Используется в docs/demo.tape, но работает и руками.
#
#   loadtest/demo.sh            # prepare + run
#   loadtest/demo.sh prepare    # сборка, инфраструктура, миграции, сервисы (долго, шумно)
#   loadtest/demo.sh run        # botsim в фоне и raidwatch на экране (чисто, для записи)
#   HP=5000000 RATE=4 loadtest/demo.sh run
#
# Чем больше HP, тем дольше живёт босс. Средний урон ~550, нагрузка — PLAYERS × RATE ударов в секунду:
# при значениях по умолчанию (50 × 3) босс с 2.5 млн HP умирает примерно за 30 секунд.
# Клавиши raidwatch (q) в Git Bash работают только через winpty: скрипт подставляет его сам в mintty.
# Чужие данные скрипт не трогает: если у первого клана уже есть активный рейд, он остановится
# и подскажет, что делать.
set -euo pipefail

STEP=${1:-all}
CLANS=1
PLAYERS=${PLAYERS:-50}
RATE=${RATE:-3}
HP=${HP:-2500000}
DURATION=${DURATION:-120s}
MODE=closed                       # closed: каждый игрок бьёт с постоянной частотой RATE
RAID_DURATION=${RAID_DURATION:-3m} # брошенный рейд закроет scheduler, не блокируя клан надолго
PLAYER=${PLAYER:-1}

# shellcheck source=loadtest/lib.sh
source "$(dirname "$0")/lib.sh"
ON_EXIT='[ -z "${BOTSIM_PID:-}" ] || kill "$BOTSIM_PID" 2>/dev/null'

prepare() {
  build_tools
  infra_up
  migrate_seed "$CLANS" "$PLAYERS"
  start_services api hits-consumer rewards-worker scheduler outbox-relay
  echo "demo environment is ready"
}

# newest_active_raid печатает id самого свежего активного рейда или ничего.
newest_active_raid() {
  psql_q "SELECT id FROM raids WHERE status = 'active' AND ends_at > now() ORDER BY started_at DESC LIMIT 1"
}

run() {
  go build -o "$BIN_DIR/raidwatch$EXE" ./cmd/raidwatch
  go build -o "$BIN_DIR/botsim$EXE" ./cmd/botsim

  local busy
  busy=$(psql_q "SELECT count(*) FROM raids WHERE status = 'active' AND clan_id = (SELECT min(id) FROM clans)")
  if [ "$busy" != "0" ]; then
    echo "у первого клана уже есть активный рейд (создан прошлым запуском): подождите, пока он закончится" \
      "(scheduler проверяет раз в 10 с, рейд демо живёт ${RAID_DURATION}) или завершите его вручную" >&2
    return 1
  fi

  "$BIN_DIR/botsim$EXE" --addr "localhost:${APP_PORT}" --clans "$CLANS" --players "$PLAYERS" \
    --hp "$HP" --duration "$DURATION" --mode "$MODE" --rate "$RATE" --raid-duration "$RAID_DURATION" \
    >"$BIN_DIR/botsim.out" 2>&1 &
  BOTSIM_PID=$!

  local raid_id=""
  for _ in $(seq 1 60); do
    raid_id=$(newest_active_raid)
    [ -n "$raid_id" ] && break
    sleep 0.5
  done
  if [ -z "$raid_id" ]; then
    echo "botsim не создал рейд за 30 с:" >&2
    cat "$BIN_DIR/botsim.out" >&2
    return 1
  fi

  # В Git Bash (mintty) нативная программа получает stdin как pipe и не видит клавиш (q не работает):
  # winpty создаёт для неё консоль. В Windows Terminal, PowerShell и vhs он не нужен.
  local runner=()
  if [ "${TERM_PROGRAM:-}" = mintty ] && command -v winpty >/dev/null; then runner=(winpty); fi
  "${runner[@]}" "$BIN_DIR/raidwatch$EXE" --addr "localhost:${APP_PORT}" --player "$PLAYER" --raid "$raid_id"
}

case "$STEP" in
  prepare) prepare ;;
  run) run ;;
  all) prepare && run ;;
  *) echo "usage: $0 [prepare|run|all]" >&2; exit 2 ;;
esac
