#!/bin/bash
# 1회 재시작 감시를 지운다. 이미 재시작이 끝났어도, 아직 대기 중이어도 동작한다.
# 대기 중에 supertrend/orchestrator를 멈춰 둔 상태라면 다시 켠다.

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
STATE="$ROOT/state"

if [[ -f "$STATE/holding" && ! -f "$STATE/done" ]]; then
  echo "멈춰 둔 supertrend와 orchestrator를 다시 켭니다."
  docker update --restart=always \
    cryptoengine-strategy-orchestrator-1 \
    cryptoengine-supertrend-1 >/dev/null || true
  docker start cryptoengine-strategy-orchestrator-1 cryptoengine-supertrend-1 >/dev/null || true
fi

if crontab -l >/tmp/bitmania-once-restart.cron 2>/dev/null; then
  awk '
    />>> bitmania-once-restart >>>/ {skip=1; next}
    /<<< bitmania-once-restart <<</ {skip=0; next}
    skip {next}
    {print}
  ' /tmp/bitmania-once-restart.cron | crontab -
  rm -f /tmp/bitmania-once-restart.cron
  echo "crontab 항목을 제거했습니다."
else
  echo "crontab 항목이 없습니다."
fi

cd /
rm -rf "$ROOT"
echo "삭제했습니다: $ROOT"
