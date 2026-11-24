#!/bin/sh
# Host watchdog for the single-process bot (the only external dead-man).
#
# Run from host cron every 5 minutes (see docs/operations.md):
#   */5 * * * * /home/justant/Data/Bit-Mania/cryptoengine/scripts/watchdog.sh >/dev/null 2>&1
#
# Checks: (1) the bot container is running and its Docker health is not "unhealthy",
#         (2) bot_state['heartbeat'] in Postgres was updated less than 25 min ago.
# On failure: one Telegram message after 2 consecutive failed runs (so a normal redeploy
# of ~1-2 min does not alert), then silence until recovery; one recovery message.
# Safe when docker or postgres is down: every external call has a timeout and a failure
# is simply a failed check. Credentials come from cryptoengine/.env (key names
# TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID only; the file is never sourced).

DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATE_DIR="$DIR/.watchdog"
STATE_FILE="$STATE_DIR/state"      # lines: status=ok|alert  fails=N
LOCK_DIR="$STATE_DIR/lock"
MAX_AGE_S=1500                      # 25 min
ALERT_AFTER_FAILS=2
export PATH="$PATH:/usr/local/bin:/usr/bin:/bin"

mkdir -p "$STATE_DIR" 2>/dev/null || exit 0
# one run at a time; a stale lock (> 10 min) is removed
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  if [ -n "$(find "$LOCK_DIR" -maxdepth 0 -mmin +10 2>/dev/null)" ]; then
    rmdir "$LOCK_DIR" 2>/dev/null; mkdir "$LOCK_DIR" 2>/dev/null || exit 0
  else
    exit 0
  fi
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null' EXIT

env_value() {  # env_value KEY -> value from .env (last assignment, quotes stripped)
  grep -E "^[[:space:]]*$1=" "$DIR/.env" 2>/dev/null | tail -n 1 | sed -E "s/^[^=]*=//; s/^[\"']//; s/[\"']\$//"
}

send_telegram() {  # send_telegram TEXT -> 0 if delivered
  token="$(env_value TELEGRAM_BOT_TOKEN)"; chat="$(env_value TELEGRAM_CHAT_ID)"
  [ -n "$token" ] && [ -n "$chat" ] || return 1
  # URL (holds the token) goes through stdin so it does not show up in `ps`
  printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$token" | \
    timeout 20 curl -sS -f -m 15 -o /dev/null -K - \
      --data-urlencode "chat_id=$chat" --data-urlencode "text=$1" >/dev/null 2>&1
}

status=ok; fails=0
if [ -f "$STATE_FILE" ]; then
  status="$(sed -n 's/^status=//p' "$STATE_FILE" | head -n 1)"
  fails="$(sed -n 's/^fails=//p' "$STATE_FILE" | head -n 1)"
fi
case "$fails" in ''|*[!0-9]*) fails=0 ;; esac
[ "$status" = alert ] || status=ok
save_state() { printf 'status=%s\nfails=%s\n' "$1" "$2" > "$STATE_FILE" 2>/dev/null; }

# ── checks ───────────────────────────────────────────────────
problem=""
cd "$DIR" 2>/dev/null || exit 0
cid="$(timeout 20 docker compose ps -q bot 2>/dev/null | head -n 1)"
if [ -z "$cid" ]; then
  problem="bot 컨테이너 없음 또는 docker 응답 없음"
else
  info="$(timeout 20 docker inspect -f '{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null)"
  running="${info%% *}"; health="${info##* }"
  if [ "$running" != "true" ]; then
    problem="bot 컨테이너가 실행 중이 아님 (state: ${info:-unknown})"
  elif [ "$health" = "unhealthy" ]; then
    problem="bot 컨테이너 unhealthy (하트비트 파일 25분 이상 갱신 없음)"
  fi
fi
if [ -z "$problem" ]; then
  age="$(timeout 20 docker compose exec -T postgres psql -U cryptoengine -d cryptoengine -tA \
         -c "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - updated_at))::bigint, -1) FROM bot_state WHERE key='heartbeat'" 2>/dev/null | head -n 1 | tr -d '[:space:]')"
  case "$age" in
    ''|*[!0-9-]*) problem="Postgres 하트비트 조회 실패 (postgres 다운 또는 응답 없음)" ;;
    -1|'') problem="bot_state 하트비트 행 없음" ;;
    *) [ "$age" -gt "$MAX_AGE_S" ] && problem="bot_state 하트비트가 ${age}초 전 (한도 ${MAX_AGE_S}초)" ;;
  esac
fi

host="$(hostname 2>/dev/null)"
now="$(date '+%Y-%m-%d %H:%M:%S %Z')"

# ── state machine ────────────────────────────────────────────
if [ -n "$problem" ]; then
  fails=$((fails + 1))
  if [ "$status" = alert ]; then
    save_state alert "$fails"            # already alerted: stay quiet
  elif [ "$fails" -ge "$ALERT_AFTER_FAILS" ]; then
    if send_telegram "[watchdog] 봇 이상 ($host, $now)
$problem
조치: docker compose ps / logs bot 확인. 포지션은 /status 또는 make emergency."; then
      save_state alert "$fails"
    else
      save_state ok "$fails"             # not delivered: retry on the next run
    fi
  else
    save_state ok "$fails"
  fi
else
  if [ "$status" = alert ]; then
    if send_telegram "[watchdog] 봇 정상 복구 ($host, $now)"; then
      save_state ok 0
    else
      save_state alert 0                 # recovery message not delivered: retry next run
    fi
  else
    save_state ok 0
  fi
fi
exit 0
