"""수동 청산 — 놓친 exit 신호 복구.

01:00 KST의 sell 신호가 implied leverage 버그로 차단된 것을 수동 체결.
실행 흐름:
  1. orders 테이블에 delay_reason / original_signal_ts 컬럼 추가 (idempotent)
  2. 시장가 sell 주문을 Redis order:request 채널에 발행
  3. order:result 구독으로 체결 확인 (최대 60초 대기)
  4. 원본 rejected 레코드에 지연 정보 기록
  5. 체결 완료된 새 주문에 delay_reason 기록
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser

import os

import asyncpg
import redis.asyncio as aioredis

# ── Connection config ─────────────────────────────────────────────────────
# 자격증명은 환경변수에서만 읽는다 (2026-08-29: 소스 하드코딩 제거).
# 미설정 시 즉시 중단한다 — 잘못된 접속으로 조용히 실패하는 것보다 낫다.
_REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
_DB_PASSWORD = os.environ.get("DB_PASSWORD")
if not _REDIS_PASSWORD or not _DB_PASSWORD:
    raise SystemExit(
        "REDIS_PASSWORD / DB_PASSWORD 환경변수가 필요하다.\n"
        "  예: set -a && . cryptoengine/.env && set +a && python scripts/manual_close_delayed.py"
    )

REDIS_URL = os.environ.get(
    "REDIS_URL", f"redis://:{_REDIS_PASSWORD}@localhost:6379"
)
PG_DSN = os.environ.get(
    "PG_DSN",
    f"postgresql://cryptoengine:{_DB_PASSWORD}@localhost:5432/cryptoengine",
)

# ── Exit order parameters ─────────────────────────────────────────────────
STRATEGY_ID   = "supertrend-01"
SYMBOL        = "BTC/USDT:USDT"
EXCHANGE      = "bybit"
QUANTITY      = 0.007           # 현재 포지션 전량

# ── 원본 놓친 신호 정보 ────────────────────────────────────────────────────
ORIGINAL_REQUEST_ID  = "3d577804d4214b07ab45cfe975ac994f"
ORIGINAL_SIGNAL_TS   = "2026-05-27T01:00:13+09:00"   # 신호 발생 시각
ORIGINAL_REJECT_REASON = "implied_leverage_exceeded: implied=5.51 > limit=3.0"

RESULT_TIMEOUT = 120  # 체결 대기 최대 초


async def ensure_delay_columns(conn: asyncpg.Connection) -> None:
    """orders 테이블에 지연 추적 컬럼 추가 (없는 경우에만)."""
    await conn.execute("""
        ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS delay_reason         TEXT,
            ADD COLUMN IF NOT EXISTS original_signal_ts   TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS original_request_id  TEXT
    """)
    print("[DB] delay 컬럼 확인/추가 완료")


async def record_original_rejection(conn: asyncpg.Connection) -> None:
    """원본 rejected 레코드에 지연 사유와 신호 시각을 기록."""
    rows = await conn.fetchval(
        "SELECT COUNT(*) FROM orders WHERE request_id = $1",
        ORIGINAL_REQUEST_ID,
    )
    if rows == 0:
        print(f"[DB] 원본 request_id {ORIGINAL_REQUEST_ID} 레코드 없음 — 스킵")
        return

    await conn.execute(
        """
        UPDATE orders
        SET delay_reason       = $2,
            original_signal_ts = $3::timestamptz,
            updated_at         = NOW()
        WHERE request_id = $1
        """,
        ORIGINAL_REQUEST_ID,
        f"safety_bug:implied_leverage_check_blocked_reduce_only | original_reject={ORIGINAL_REJECT_REASON}",
        dateutil_parser.parse(ORIGINAL_SIGNAL_TS),
    )
    print(f"[DB] 원본 rejected 레코드 delay_reason 기록 완료 ({ORIGINAL_REQUEST_ID})")


async def wait_for_result(
    redis: aioredis.Redis,
    request_id: str,
    timeout: float,
) -> dict | None:
    """order:result 채널에서 특정 request_id의 결과를 기다린다."""
    pubsub = redis.pubsub()
    await pubsub.subscribe("order:result", f"order:result:{STRATEGY_ID}")
    deadline = asyncio.get_event_loop().time() + timeout
    try:
        while asyncio.get_event_loop().time() < deadline:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is None:
                continue
            try:
                data = json.loads(msg["data"])
                if data.get("request_id") == request_id:
                    return data
            except Exception:
                pass
        return None
    finally:
        await pubsub.unsubscribe()
        await pubsub.aclose()


async def main() -> None:
    print("=" * 60)
    print("수동 청산 — 01:00 KST exit 신호 지연 복구")
    print(f"  신호 발생:  {ORIGINAL_SIGNAL_TS}")
    print(f"  실행 시각:  {datetime.now(timezone.utc).isoformat()}")
    print(f"  심볼/수량:  {SYMBOL} × {QUANTITY} BTC")
    print("=" * 60)

    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    pg    = await asyncpg.connect(PG_DSN)

    try:
        # 1. DB 컬럼 준비
        await ensure_delay_columns(pg)

        # 2. 원본 rejected 레코드에 사유 기록
        await record_original_rejection(pg)

        # 3. 새 시장가 청산 주문 생성
        new_request_id = uuid.uuid4().hex
        executed_at    = datetime.now(timezone.utc).isoformat()

        payload = {
            "strategy_id": STRATEGY_ID,
            "exchange":    EXCHANGE,
            "symbol":      SYMBOL,
            "side":        "sell",
            "order_type":  "market",
            "quantity":    QUANTITY,
            "price":       None,
            "post_only":   False,
            "reduce_only": True,
            "request_id":  new_request_id,
        }

        print(f"\n[ORDER] 시장가 sell 발행 — request_id={new_request_id}")
        await redis.publish("order:request", json.dumps(payload))

        # 4. 체결 대기
        print(f"[ORDER] 체결 대기 중 (최대 {RESULT_TIMEOUT}초)...")
        result = await wait_for_result(redis, new_request_id, RESULT_TIMEOUT)

        if result is None:
            print("[ERROR] 체결 결과 수신 실패 (타임아웃)")
            sys.exit(1)

        status        = result.get("status")
        filled_qty    = result.get("filled_qty", 0.0)
        filled_price  = result.get("filled_price")
        order_id      = result.get("order_id", "")

        print(f"\n[RESULT] status={status} | filled_qty={filled_qty} | filled_price={filled_price}")
        print(f"         order_id={order_id}")

        # 5. 새 주문 레코드에 delay 정보 기록
        await pg.execute(
            """
            UPDATE orders
            SET delay_reason         = $2,
                original_signal_ts   = $3::timestamptz,
                original_request_id  = $4,
                updated_at           = NOW()
            WHERE request_id = $1
            """,
            new_request_id,
            (
                f"delayed_exit: original_signal={ORIGINAL_SIGNAL_TS} "
                f"| rejection={ORIGINAL_REJECT_REASON} "
                f"| executed_at={executed_at}"
            ),
            dateutil_parser.parse(ORIGINAL_SIGNAL_TS),
            ORIGINAL_REQUEST_ID,
        )
        print(f"[DB] 새 주문 delay_reason 기록 완료")

        # 6. 원본 rejected 레코드의 status를 'filled_delayed'로 변경
        if status == "filled":
            await pg.execute(
                """
                UPDATE orders
                SET status     = 'filled_delayed',
                    updated_at = NOW()
                WHERE request_id = $1
                """,
                ORIGINAL_REQUEST_ID,
            )
            print(f"[DB] 원본 레코드 status: rejected → filled_delayed")

            # 신호 테이블 exit_signal 업데이트 (ON CONFLICT로 hold가 기록된 경우 덮어씀)
            await pg.execute(
                """
                UPDATE supertrend_signals
                SET exit_signal    = true,
                    exit_reason    = 'ema_cross',
                    expected_action = 'exit'
                WHERE bar_ts = '2026-05-26 16:00:00+00'
                  AND exit_signal = false
                """,
            )
            print("[DB] supertrend_signals 01:00 bar: exit_signal=true, expected_action=exit 업데이트")

        print("\n" + "=" * 60)
        print("완료 요약")
        print(f"  체결 상태:       {status}")
        print(f"  체결 가격:       {filled_price}")
        print(f"  체결 수량:       {filled_qty} BTC")
        print(f"  신호 발생:       {ORIGINAL_SIGNAL_TS}")
        print(f"  실제 체결 시각:  {executed_at}")
        if filled_price and filled_qty:
            import ast
            # entry price from position was 77345.33
            entry_price = 77345.32857143
            pnl = (filled_price - entry_price) * filled_qty
            delay_hours = (
                datetime.now(timezone.utc)
                - datetime.fromisoformat(ORIGINAL_SIGNAL_TS)
            ).total_seconds() / 3600
            print(f"  진입가:          {entry_price:.2f}")
            print(f"  청산가:          {filled_price:.2f}")
            print(f"  실현 PnL:        {pnl:.4f} USDT")
            print(f"  지연 시간:       {delay_hours:.1f}h")
        print("=" * 60)

    finally:
        await redis.aclose()
        await pg.close()


if __name__ == "__main__":
    asyncio.run(main())
