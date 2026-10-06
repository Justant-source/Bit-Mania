#!/bin/bash
# 임시 1회 작업. 열린 포지션이 확인된 뒤, 그 포지션이 닫히면
# Bit-Mania 컨테이너를 한 번만 재시작한다.
#
# 삭제:
#   /home/justant/Data/Bit-Mania/cryptoengine/ops/once-restart-after-close/remove.sh
#
# 포지션이 열려 있는 동안에는 재시작하지 않는다.
# ce:positions:all 이 없거나 파싱에 실패하면 닫힌 것으로 보지 않는다.

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
STATE="$ROOT/state"
LOG="$ROOT/run.log"
mkdir -p "$STATE"

log() {
  printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$LOG"
}

if [[ -f "$STATE/done" ]]; then
  exit 0
fi

exec 9>"$STATE/lock"
if ! flock -n 9; then
  exit 0
fi

read_positions() {
  docker exec cryptoengine-redis-1 \
    sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning GET ce:positions:all' 2>/dev/null || true
}

# 출력: open | flat | unknown
classify() {
  local raw="$1"
  RAW="$raw" python3 - <<'PY'
import json, os, sys
raw = os.environ.get("RAW", "")
text = raw.strip()
if not text or text == "(nil)":
    print("unknown")
    sys.exit(0)
try:
    data = json.loads(text)
except json.JSONDecodeError:
    print("unknown")
    sys.exit(0)
if isinstance(data, dict):
    rows = data.get("positions", [])
elif isinstance(data, list):
    rows = data
else:
    print("unknown")
    sys.exit(0)
try:
    open_n = sum(1 for p in rows if float(p.get("size") or 0) != 0)
except (TypeError, ValueError):
    print("unknown")
    sys.exit(0)
print("open" if open_n else "flat")
PY
}

restore_watchers() {
  docker update --restart=always \
    cryptoengine-strategy-orchestrator-1 \
    cryptoengine-supertrend-1 >/dev/null
  docker start cryptoengine-strategy-orchestrator-1 cryptoengine-supertrend-1 >/dev/null
}

pause_watchers() {
  docker update --restart=no \
    cryptoengine-strategy-orchestrator-1 \
    cryptoengine-supertrend-1 >/dev/null
  docker stop -t 30 cryptoengine-strategy-orchestrator-1 cryptoengine-supertrend-1 >/dev/null
}

restart_all_once() {
  local names=()
  mapfile -t names < <(docker ps -a \
    --filter label=com.docker.compose.project=cryptoengine \
    --format '{{.Names}}' | grep -v '^cryptoengine-strategy-orchestrator-1$' || true)
  if [[ ${#names[@]} -eq 0 ]]; then
    log "재시작 대상이 없습니다"
    return 1
  fi
  log "orchestrator를 끈 채로 재시작: ${names[*]}"
  docker restart "${names[@]}"
  local i health
  health="missing"
  for i in $(seq 1 24); do
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' cryptoengine-execution-engine-1 2>/dev/null || echo missing)"
    if [[ "$health" == "healthy" || "$health" == "none" ]]; then
      break
    fi
    sleep 10
  done
  if [[ "$health" != "healthy" && "$health" != "none" ]]; then
    log "execution-engine 상태가 ${health} 입니다. orchestrator는 올리지 않습니다."
    return 1
  fi
  docker update --restart=always cryptoengine-strategy-orchestrator-1 cryptoengine-supertrend-1 >/dev/null
  docker start cryptoengine-strategy-orchestrator-1 cryptoengine-supertrend-1 >/dev/null
  if docker ps -aq --filter name=^bitmania-dashboard-dashboard-1$ | grep -q .; then
    docker restart bitmania-dashboard-dashboard-1 >/dev/null
  fi
  log "재시작 완료. execution-engine=${health}"
}

raw="$(read_positions)"
kind="$(classify "$raw")"
log "positions=${kind}"

if [[ -f "$STATE/holding" ]]; then
  if [[ "$kind" != "flat" ]]; then
    log "확인 중 포지션이 다시 열림. 감시 컨테이너를 되돌리고 다음 청산을 기다립니다."
    restore_watchers
    rm -f "$STATE/holding"
    exit 0
  fi
  log "포지션이 비어 있음을 다시 확인. 1회 재시작을 진행합니다."
  if restart_all_once; then
    date -Is > "$STATE/done"
    rm -f "$STATE/holding"
    log "done. 이 작업은 다시 재시작하지 않습니다. 삭제는 remove.sh"
    exit 0
  fi
  log "재시작 실패. 감시 컨테이너를 되돌리고 이 작업은 더 이상 재시작하지 않습니다."
  restore_watchers || true
  date -Is > "$STATE/failed"
  rm -f "$STATE/holding"
  exit 1
fi

if [[ -f "$STATE/failed" ]]; then
  exit 0
fi

case "$kind" in
  open)
    date -Is > "$STATE/armed"
    rm -f "$STATE/flat_once"
    ;;
  flat)
    if [[ ! -f "$STATE/armed" ]]; then
      log "아직 열린 포지션을 본 적이 없습니다. 재시작하지 않습니다."
      exit 0
    fi
    if [[ ! -f "$STATE/flat_once" ]]; then
      date -Is > "$STATE/flat_once"
      log "포지션이 비었습니다. 재진입을 막기 위해 supertrend와 orchestrator를 잠시 멈춥니다."
      pause_watchers
      date -Is > "$STATE/holding"
      exit 0
    fi
    ;;
  *)
    rm -f "$STATE/flat_once"
    log "포지션 캐시를 신뢰할 수 없습니다. 재시작하지 않습니다."
    ;;
esac
exit 0
