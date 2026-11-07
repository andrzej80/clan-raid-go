#!/usr/bin/env bash
# Проверка правил алертов: синтаксис (vmalert -dryRun) и юнит-тесты (vmalert-tool unittest).
# Нужен только Docker. Версия — та же, что у VictoriaMetrics в docker-compose.
#
#   deploy/alerts/test.sh
set -euo pipefail

VM_VERSION=${VM_VERSION:-v1.153.0}
export MSYS_NO_PATHCONV=1 # Git Bash на Windows иначе портит пути внутри контейнера
DIR=$(cd "$(dirname "$0")" && pwd)
if command -v cygpath >/dev/null; then DIR=$(cygpath -m "$DIR"); fi

docker run --rm -v "$DIR:/rules:ro" "victoriametrics/vmalert:${VM_VERSION}" \
  -rule="/rules/*.yml" -dryRun -loggerLevel=ERROR
docker run --rm -v "$DIR:/rules:ro" "victoriametrics/vmalert-tool:${VM_VERSION}" \
  unittest --disableAlertgroupLabel --files="/rules/tests/*.yml"
