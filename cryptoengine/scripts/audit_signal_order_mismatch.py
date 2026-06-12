"""신호-주문 대조 감사 — supertrend_signals(기대 행동) vs orders(실제 체결).

전략이 매 봉 기록하는 expected_action(enter/exit)에 대응하는 체결 주문이
실제로 존재하는지 대조한다. 미체결 사고(예: 2026-05-27 청산 차단)를
사후가 아닌 일상 점검으로 잡기 위한 도구.

검사 항목:
  1. signal_no_fill   — enter/exit 신호 후 체결 창(기본 30분) 내 체결 주문 없음
  2. qty_mismatch     — 체결 수량이 기대 수량과 ±5% 초과 차이 (enter만)
  3. unmatched_fill   — 신호 없이 체결된 주문 (수동 개입/유령 주문 탐지)

사용법 (호스트에서, cryptoengine/.env 자동 로드):
  python3 cryptoengine/scripts/audit_signal_order_mismatch.py            # 전체 이력
  python3 cryptoengine/scripts/audit_signal_order_mismatch.py --days 7   # 최근 7일
  python3 cryptoengine/scripts/audit_signal_order_mismatch.py --alert    # 불일치 시 Telegram 알림 발행

종료 코드: 0 = 불일치 없음, 1 = 불일치 발견, 2 = 실행 오류
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg

# 신호 bar_ts는 봉 시작 시각 — 신호는 봉 마감(+4h)에 발생한다
BAR_DURATION = timedelta(hours=4)
# 체결 창: 봉 마감 후 재페그(~3.5분) + 폴백 + exit 재시도(60s) 여유
FILL_WINDOW = timedelta(minutes=30)
QTY_TOLERANCE = 0.05  # ±5%
STRATEGY_PREFIX = "supertrend"

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _load_env_file() -> None:
    """cryptoengine/.env에서 미설정 변수만 보충한다 (dotenv 의존성 없이)."""
    if not ENV_PATH.exists():
        return
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _pg_dsn() -> str:
    return (
        f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
        f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
        f"@{os.getenv('DB_HOST', '127.0.0.1')}"
        f":{os.getenv('DB_PORT', '5432')}"
        f"/{os.getenv('DB_NAME', 'cryptoengine')}"
    )


async def _publish_alert(message: str) -> None:
    """불일치 요약을 ce:alerts:anomaly 채널로 발행 (telegram-bot이 중계)."""
    try:
        import redis.asyncio as aioredis

        url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
        client = aioredis.from_url(url, decode_responses=True)
        await client.publish(
            "ce:alerts:anomaly",
            json.dumps({
                "type": "anomaly",
                "severity": "critical",
                "message": message,
            }),
        )
        await client.aclose()
        print("[alert] ce:alerts:anomaly 발행 완료")
    except Exception as exc:
        print(f"[alert] 발행 실패: {exc}")


async def audit(days: int | None, alert: bool) -> int:
    conn = await asyncpg.connect(_pg_dsn())
    mismatches: list[str] = []
    try:
        since = (
            datetime.now(timezone.utc) - timedelta(days=days)
            if days
            else datetime(2000, 1, 1, tzinfo=timezone.utc)
        )

        # computed_at이 봉 마감(+4h)에서 6시간 이내인 라이브 계산 신호만 대조한다.
        # backfill_supertrend_signals.py가 넣은 과거 신호(computed_at = 백필 실행일)는
        # 실거래 이전이므로 대응 주문이 없는 게 정상 — 제외.
        signals = await conn.fetch(
            """
            SELECT bar_ts, expected_action, expected_qty
            FROM supertrend_signals
            WHERE expected_action IN ('enter', 'exit')
              AND bar_ts >= $1
              AND computed_at < bar_ts + INTERVAL '6 hours'
            ORDER BY bar_ts
            """,
            since,
        )

        fills = await conn.fetch(
            """
            SELECT request_id, side, filled_qty, filled_price, quantity,
                   created_at, updated_at, reduce_only
            FROM orders
            WHERE status = 'filled'
              AND (strategy_id LIKE $1 OR strategy_id IS NULL)
              AND created_at >= $2
            ORDER BY created_at
            """,
            f"{STRATEGY_PREFIX}%",
            since,
        )

        matched_fill_ids: set[str] = set()

        # 1·2) 신호별 체결 존재·수량 대조
        for sig in signals:
            signal_at = sig["bar_ts"] + BAR_DURATION  # 봉 마감 = 신호 발생 시각
            window_end = signal_at + FILL_WINDOW
            want_side = "buy" if sig["expected_action"] == "enter" else "sell"

            window_fills = [
                f for f in fills
                if f["side"] == want_side and signal_at <= f["created_at"] <= window_end
            ]

            if not window_fills:
                mismatches.append(
                    f"signal_no_fill: {sig['bar_ts']:%Y-%m-%d %H:%M}Z "
                    f"action={sig['expected_action']} expected_qty={sig['expected_qty']}"
                )
                continue

            for f in window_fills:
                matched_fill_ids.add(f["request_id"])

            if sig["expected_action"] == "enter" and sig["expected_qty"]:
                total = sum(float(f["filled_qty"] or 0) for f in window_fills)
                expected = float(sig["expected_qty"])
                # 축소 진입(equity < 할당자본)은 정상이므로 하한만 넉넉히 본다
                if expected > 0 and abs(total - expected) / expected > QTY_TOLERANCE:
                    mismatches.append(
                        f"qty_mismatch: {sig['bar_ts']:%Y-%m-%d %H:%M}Z "
                        f"expected={expected:.6f} filled={total:.6f} "
                        f"(축소 진입이면 entry_capital_reduced 로그 확인)"
                    )

        # 3) 신호와 매칭되지 않은 체결 (수동 개입·유령 주문)
        for f in fills:
            if f["request_id"] not in matched_fill_ids:
                mismatches.append(
                    f"unmatched_fill: {f['created_at']:%Y-%m-%d %H:%M}Z "
                    f"side={f['side']} qty={f['filled_qty']} reduce_only={f['reduce_only']} "
                    f"request_id={f['request_id'][:12]}… (수동 개입 또는 on_stop 청산일 수 있음)"
                )

        print(f"검사 구간: {since:%Y-%m-%d} ~ 현재")
        print(f"신호(enter/exit): {len(signals)}건, 체결 주문: {len(fills)}건")
        if mismatches:
            print(f"\n⚠️  불일치 {len(mismatches)}건:")
            for m in mismatches:
                print(f"  - {m}")
            if alert:
                summary = "\n".join(mismatches[:10])
                await _publish_alert(
                    f"📋 신호-주문 감사 불일치 {len(mismatches)}건\n{summary}"
                )
            return 1
        print("\n✅ 불일치 없음 — 모든 신호가 체결로 이어짐")
        return 0
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="supertrend 신호-주문 대조 감사")
    parser.add_argument("--days", type=int, default=None, help="최근 N일만 검사 (기본: 전체)")
    parser.add_argument("--alert", action="store_true", help="불일치 시 Telegram 알림 발행")
    args = parser.parse_args()

    _load_env_file()
    try:
        return asyncio.run(audit(args.days, args.alert))
    except Exception as exc:
        print(f"실행 오류: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
