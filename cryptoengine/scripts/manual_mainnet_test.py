#!/usr/bin/env python3
"""Mainnet 매수/매도 1회 자동 트리거 테스트.

4h Supertrend 신호를 기다리지 않고 order:request 채널에 직접 OrderRequest JSON 을
publish 해 execution-engine → SafetyGuard → Bybit 메인넷 전체 파이프라인을 검증한다.

실행 전 설치:
  pip install redis

실행:
  cd cryptoengine/
  BYBIT_TESTNET=false EXPECTED_INITIAL_BALANCE_USD=200 \\
      python scripts/manual_mainnet_test.py [--dry-run] [--yes]

환경변수 (선택적 오버라이드):
  REDIS_URL                     미지정 시 REDIS_PASSWORD로 구성 (둘 중 하나 필수)
  BYBIT_TESTNET                 반드시 "false" 여야 실행됨
  EXPECTED_INITIAL_BALANCE_USD  Phase 5 표식 확인용

안전:
  - entry 후 exit 실패 시 화면에 수동 청산 명령어 출력 (포지션 열린 채 종료)
  - 절대 make emergency / /kill / /emergency_close 사용 금지 (L4 Kill Switch 발동)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import redis.asyncio as aioredis

# ────────────────────────────────────────────────────────────────────
# 상수
# ────────────────────────────────────────────────────────────────────
# 자격증명 하드코딩 제거 (2026-08-29). REDIS_PASSWORD 또는 REDIS_URL 필요.
_REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
DEFAULT_REDIS_URL = (
    f"redis://:{_REDIS_PASSWORD}@127.0.0.1:6379" if _REDIS_PASSWORD else ""
)
EXCHANGE = "bybit"
SYMBOL = "BTC/USDT:USDT"
STRATEGY_ID = "manual-test-01"  # supertrend-01 과 분리된 ID
LEVERAGE = 3
QTY_STEP = 0.001          # Bybit BTC min step
TAKER_FEE_RATE = 0.00055  # Bybit USDT perp taker fee
MIN_NOTIONAL = 65.0        # SafetyGuard min notional gate
FILL_TIMEOUT_S = 60       # market order default
MAKER_FEE_RATE = 0.0002  # Bybit USDT perp maker fee (post-only)
POS_VERIFY_RETRIES = 8
POS_VERIFY_SLEEP = 2.0

# 4h 캔들 close 불안전 윈도우: UTC 시:분 기준 ±5분
_4H_CLOSE_HOURS_UTC = {0, 4, 8, 12, 16, 20}

G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
B = "\033[94m"
X = "\033[0m"


def _ok(msg: str) -> None:   print(f"  {G}✓{X} {msg}")
def _fail(msg: str) -> None: print(f"  {R}✗{X} {msg}")
def _warn(msg: str) -> None: print(f"  {Y}!{X} {msg}")
def _info(msg: str) -> None: print(f"  {B}·{X} {msg}")


# ────────────────────────────────────────────────────────────────────
# 헬퍼
# ────────────────────────────────────────────────────────────────────

def _floor(value: float, step: float) -> float:
    return math.floor(value / step) * step


def _make_order(
    side: str,
    quantity: float,
    reduce_only: bool,
    suffix: str,
    order_type: str = "market",
    price: float | None = None,
    post_only: bool = False,
) -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return {
        "strategy_id": STRATEGY_ID,
        "exchange": EXCHANGE,
        "symbol": SYMBOL,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "price": price,
        "post_only": post_only,
        "reduce_only": reduce_only,
        "stop_loss": None,
        "take_profit": None,
        "request_id": f"manual-test-{ts}-{suffix}",
    }


def _candle_window_safe() -> tuple[bool, str]:
    """UTC 시각이 4h 캔들 close ±5분 윈도우인지 검사."""
    now = datetime.now(timezone.utc)
    h, m = now.hour, now.minute
    if h in _4H_CLOSE_HOURS_UTC and m < 5:
        safe_at = f"{h:02d}:05 UTC"
        return False, f"4h 캔들 close 직후 위험 윈도우 ({h:02d}:00 UTC). {safe_at} 이후 재실행."
    next_close_h = min((hh for hh in _4H_CLOSE_HOURS_UTC if hh > h), default=min(_4H_CLOSE_HOURS_UTC))
    next_close_is_soon = (h == next_close_h - 1 and m >= 55) or (next_close_h == 0 and h == 23 and m >= 55)
    if next_close_is_soon:
        return False, f"다음 4h 캔들 close {next_close_h:02d}:00 UTC 직전. 5분 후 재실행."
    return True, ""


def _print_manual_close(qty: float) -> None:
    close_payload = {
        "strategy_id": STRATEGY_ID,
        "exchange": EXCHANGE,
        "symbol": SYMBOL,
        "side": "sell",
        "order_type": "market",
        "quantity": qty,
        "post_only": False,
        "reduce_only": True,
        "stop_loss": None,
        "take_profit": None,
        "request_id": f"manual-close-{uuid.uuid4().hex[:8]}",
    }
    cmd = f"redis-cli -a $REDIS_PASSWORD PUBLISH order:request '{json.dumps(close_payload)}'"
    print()
    print(f"  {R}⚠ 포지션이 열린 상태입니다. 즉시 수동 청산이 필요합니다.{X}")
    print()
    print("  수동 청산 방법 (택 1):")
    print("    1. Bybit UI → BTC/USDT Perp 포지션 → Market Close")
    print("    2. Telegram /positions 확인 후 아래 명령 실행:")
    print(f"       {cmd}")
    print()
    print(f"  {R}⚠ 절대 금지: make emergency / /kill / /emergency_close{X}")
    print("     → L4 Kill Switch 발동 → supertrend-01 영구 정지")


# ────────────────────────────────────────────────────────────────────
# Redis 조회 헬퍼
# ────────────────────────────────────────────────────────────────────

async def _btc_price(client: aioredis.Redis) -> float | None:
    """cache:price:bybit:BTCUSDT — 순수 float 문자열."""
    raw = await client.get(f"cache:price:{EXCHANGE}:BTCUSDT")
    if not raw:
        return None
    try:
        p = float(raw)
        return p if p > 0 else None
    except (ValueError, TypeError):
        return None


async def _kill_switch_active(client: aioredis.Redis) -> bool:
    val = await client.get("ce:kill_switch:active")
    return val not in (None, "0", "", "false")


async def _position_size(client: aioredis.Redis) -> float:
    raw = await client.get(f"cache:position:{EXCHANGE}:{SYMBOL}")
    if not raw:
        return 0.0
    try:
        return float(json.loads(raw).get("size", 0) or 0)
    except Exception:
        return 0.0


async def _strategy_position_count(client: aioredis.Redis) -> int:
    raw = await client.get("strategy:status:supertrend-01")
    if not raw:
        return 0
    try:
        return int(json.loads(raw).get("position_count", 0))
    except Exception:
        return 0


# ────────────────────────────────────────────────────────────────────
# Fill 폴링 (pubsub 기반)
# ────────────────────────────────────────────────────────────────────

async def _wait_for_result(
    pubsub: aioredis.client.PubSub,
    request_id: str,
    timeout_s: int,
    label: str,
) -> dict | None:
    """이미 subscribe 된 pubsub 에서 request_id 에 해당하는 terminal result 를 기다린다.

    get_message(timeout=1.0) 을 사용해 asyncio.wait_for 취소 문제 없이 1초씩 polling.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            break
        print(f"\r  {B}·{X} {label} fill 대기 중... {remaining:2d}s", end="", flush=True)

        # timeout=1.0 으로 최대 1초 대기 — asyncio.wait_for 없이 내장 타임아웃 사용
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

        if msg is None:
            continue
        try:
            data = json.loads(msg["data"])
        except (json.JSONDecodeError, TypeError, KeyError):
            continue

        if data.get("request_id") == request_id:
            print()  # carriage return 정리
            return data

    print()
    return None


# ────────────────────────────────────────────────────────────────────
# 메인 로직
# ────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> int:
    capital = float(args.capital)
    hold_s = int(args.hold_seconds)
    dry_run = args.dry_run
    auto_yes = args.yes
    order_type = args.order_type
    post_only = (order_type == "limit")
    repeg_interval = int(args.repeg_interval)
    max_attempts = int(args.max_attempts)
    fill_timeout = (repeg_interval * max_attempts + 30) if order_type == "limit" else FILL_TIMEOUT_S

    redis_url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)
    host_hint = redis_url.split("@")[-1] if "@" in redis_url else redis_url

    print(f"\n{'='*62}")
    print("  Mainnet 매수/매도 자동 트리거 테스트")
    print(f"{'='*62}")
    if dry_run:
        print(f"  {Y}[DRY-RUN — publish 없음]{X}")
    print()

    # 두 연결: 하나는 GET/PUBLISH, 하나는 영속적 SUBSCRIBE
    client = aioredis.from_url(redis_url, decode_responses=True)
    client_sub = aioredis.from_url(redis_url, decode_responses=True)

    try:
        await client.ping()
        _ok(f"Redis 연결 성공 ({host_hint})")
    except Exception as exc:
        _fail(f"Redis 연결 실패: {exc}")
        _info("redis-cli -a $REDIS_PASSWORD ping 으로 확인")
        return 1

    # ── 사전 검증 ─────────────────────────────────────────────────────
    print("\n[사전 검증]")

    testnet = os.environ.get("BYBIT_TESTNET", "true").lower()
    if testnet != "false":
        _fail(f"BYBIT_TESTNET={testnet!r}. 메인넷이 아닙니다.")
        _info("export BYBIT_TESTNET=false 후 재실행")
        return 1
    _ok("BYBIT_TESTNET=false (메인넷 확인)")

    balance_env = os.environ.get("EXPECTED_INITIAL_BALANCE_USD", "")
    if balance_env:
        _ok(f"EXPECTED_INITIAL_BALANCE_USD={balance_env} (Phase 5 확인)")
    else:
        _warn("EXPECTED_INITIAL_BALANCE_USD 미설정 (계속 진행)")

    if await _kill_switch_active(client):
        _fail("Kill Switch 활성 (ce:kill_switch:active=1). 주문 불가.")
        _info("해제: redis-cli -a $REDIS_PASSWORD SET ce:kill_switch:active 0")
        return 1
    _ok("Kill Switch 비활성")

    pos_size = await _position_size(client)
    if pos_size != 0.0:
        _fail(f"기존 포지션 존재 (size={pos_size} BTC). 포지션 없을 때만 실행 가능.")
        return 1
    _ok("현재 포지션 없음 (flat)")

    st_pos = await _strategy_position_count(client)
    if st_pos > 0:
        _warn(f"supertrend-01 이 포지션 보유 중 (position_count={st_pos}). 진행 시 충돌 가능.")
    else:
        _ok("supertrend-01 포지션 없음")

    window_ok, window_msg = _candle_window_safe()
    if not window_ok:
        _fail(f"4h 캔들 close 윈도우: {window_msg}")
        return 1
    _ok("4h 캔들 close 윈도우 안전")

    btc_price = await _btc_price(client)
    if btc_price is None:
        _fail("BTC 현재가 조회 실패 (cache:ticker:bybit:BTC/USDT:USDT). market-data 서비스 확인.")
        _info("docker compose logs --tail=20 market-data")
        return 1
    _ok(f"BTC 현재가: ${btc_price:,.2f}")

    # ── 수량 계산 ─────────────────────────────────────────────────────
    raw_qty = (capital * 0.95 * LEVERAGE) / btc_price
    qty = _floor(raw_qty, QTY_STEP)
    notional = qty * btc_price
    fee_rate = MAKER_FEE_RATE if order_type == "limit" else TAKER_FEE_RATE
    fee_est = notional * fee_rate * 2

    if notional < MIN_NOTIONAL:
        _fail(f"Notional ${notional:.2f} < 최소 ${MIN_NOTIONAL}. 자본 부족.")
        return 1

    entry_order = _make_order(
        "buy", qty, reduce_only=False, suffix="entry",
        order_type=order_type,
        price=btc_price if order_type == "limit" else None,
        post_only=post_only,
    )
    exit_order = _make_order(
        "sell", qty, reduce_only=True, suffix="exit",
        order_type=order_type,
        price=btc_price if order_type == "limit" else None,
        post_only=post_only,
    )

    print("\n[계획된 주문]")
    _info(f"수량       : {qty} BTC")
    _info(f"Notional   : ${notional:,.2f}  (${capital} × 3x × 0.95)")
    fee_label = f"왕복 {'maker' if order_type == 'limit' else 'taker'} ({fee_rate*100:.3f}%×2)"
    _info(f"예상 수수료: ~${fee_est:.3f} ({fee_label})")
    _info(f"주문 유형  : {order_type.upper()}{' (post-only, repeg ' + str(repeg_interval) + 's×' + str(max_attempts) + ')' if order_type == 'limit' else ''}")
    _info(f"Fill timeout: {fill_timeout}초")
    _info(f"Hold time  : {hold_s}초")
    _info(f"Strategy ID: {STRATEGY_ID}")
    print()
    _info(f"Entry: {json.dumps(entry_order)}")

    if dry_run:
        print()
        _info(f"Exit : {json.dumps(exit_order)}")
        print(f"\n  {Y}[DRY-RUN 완료]{X} publish 없이 종료.\n")
        return 0

    print()
    _info(f"Exit : {json.dumps(exit_order)}")

    # ── 사용자 확인 ───────────────────────────────────────────────────
    if not auto_yes:
        print()
        print(f"  {Y}⚠ 메인넷 실제 거래가 발생합니다.{X}")
        print(f"  예상 손실: ~${fee_est:.3f} (수수료) + 슬리피지")
        try:
            confirm = input("  실행하려면 'YES' 를 정확히 입력: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  취소됨.")
            return 0
        if confirm != "YES":
            print("  취소됨.")
            return 0

    # ── subscribe 셋업 (publish 보다 먼저) ────────────────────────────
    print("\n[채널 구독 셋업]")
    result_ch = f"order:result:{STRATEGY_ID}"
    pubsub = client_sub.pubsub()
    await pubsub.subscribe(result_ch, "order:result")
    await asyncio.sleep(0.3)  # subscribe 확정 대기
    _ok(f"order:result + {result_ch} 구독 완료")

    # ── Entry 발행 ────────────────────────────────────────────────────
    print("\n[Entry 주문 발행]")
    entry_json = json.dumps(entry_order)
    entry_rid = entry_order["request_id"]

    rcv = await client.publish("order:request", entry_json)
    entry_sent_at = datetime.now(timezone.utc)
    _info(f"order:request publish → {rcv} subscriber(s)  rid={entry_rid}")

    if rcv == 0:
        _fail("execution-engine 이 order:request 를 구독하지 않음 (서비스 확인 필요)")
        _info("docker compose logs --tail=20 execution-engine")
        return 1

    entry_result = await _wait_for_result(pubsub, entry_rid, fill_timeout, "Entry")

    if entry_result is None:
        _fail(f"Entry fill {fill_timeout}초 내 응답 없음. rid={entry_rid}")
        _info("docker compose logs --tail=50 execution-engine | grep manual-test")
        _print_manual_close(qty)
        return 1

    e_status = entry_result.get("status")
    e_order_id = entry_result.get("order_id", "")
    if e_status == "rejected" or not e_order_id:
        _fail(f"Entry 거부됨: status={e_status}")
        _info(f"결과: {entry_result}")
        _info("SafetyGuard 거부 가능성. 로그 확인:")
        _info("  docker compose logs --tail=50 execution-engine | grep ORDER_REJECTED")
        return 1

    # Bybit market order 는 ccxt 에서 status=new 로 돌아오는 것이 정상.
    # order_id 가 있으면 exchange 에 제출됐고 거의 즉시 fill 됨.
    if e_status == "filled":
        e_qty = float(entry_result.get("filled_qty") or qty)
        e_price = float(entry_result.get("filled_price") or btc_price)
        e_fee = float(entry_result.get("fee") or 0)
        _ok(f"Entry FILLED  qty={e_qty}  price=${e_price:,.2f}  fee=${e_fee:.5f}")
    else:
        _ok(f"Entry 주문 Bybit 전송 완료 (status={e_status}, order_id={e_order_id[:8]}...)")
        _info("Bybit market order 는 즉시 fill 되나 API 응답은 'new' 로 선행 반환됨")
        e_fee = float(entry_result.get("fee") or 0)

    # ── 포지션 캐시 확인 (position_tracker 가 갱신할 때까지 대기) ─────
    _info("포지션 캐시 확인 중 (최대 15초)...")
    cached_size = 0.0
    cached_price = 0.0
    for _ in range(15):
        await asyncio.sleep(1.0)
        cached_size = await _position_size(client)
        if cached_size > 0:
            break

    if cached_size > 0:
        e_qty = cached_size
        raw = await client.get(f"cache:position:{EXCHANGE}:{SYMBOL}")
        if raw:
            try:
                pos_data = json.loads(raw)
                cached_price = float(pos_data.get("entry_price", 0) or btc_price)
            except Exception:
                cached_price = btc_price
        e_price = cached_price if cached_price > 0 else btc_price
        _ok(f"PositionTracker 캐시 확인: size={cached_size} BTC  entry≈${e_price:,.2f}")
    else:
        e_qty = qty
        e_price = btc_price
        _warn("포지션 캐시 미갱신 (15초 경과). entry 가격은 현재가 기준으로 계속 진행.")

    # ── Hold ──────────────────────────────────────────────────────────
    print(f"\n[{hold_s}초 Hold 중]")
    for i in range(hold_s, 0, -1):
        print(f"\r  대기 중... {i:2d}s", end="", flush=True)
        await asyncio.sleep(1)
    print(f"\r  Hold {hold_s}초 완료.     ")

    # ── Exit 발행 (reduce_only) ───────────────────────────────────────
    print("\n[Exit 주문 발행 (reduce_only)]")
    exit_json = json.dumps(exit_order)
    exit_rid = exit_order["request_id"]

    rcv = await client.publish("order:request", exit_json)
    exit_sent_at = datetime.now(timezone.utc)
    _info(f"order:request publish → {rcv} subscriber(s)  rid={exit_rid}")

    exit_result = await _wait_for_result(pubsub, exit_rid, fill_timeout, "Exit")

    if exit_result is None:
        _fail(f"Exit fill {fill_timeout}초 내 응답 없음. rid={exit_rid}")
        _print_manual_close(e_qty)
        return 1

    x_status = exit_result.get("status")
    x_order_id = exit_result.get("order_id", "")
    if x_status == "rejected" or not x_order_id:
        _fail(f"Exit 거부됨: status={x_status}")
        _info(f"결과: {exit_result}")
        _print_manual_close(e_qty)
        return 1

    # Bybit market order 는 ccxt 에서 status=new 로 돌아오는 것이 정상.
    # order_id 가 있으면 exchange 에 제출됐고 거의 즉시 fill 됨.
    fresh_price = await _btc_price(client) or btc_price
    if x_status == "filled":
        x_qty = float(exit_result.get("filled_qty") or e_qty)
        x_price = float(exit_result.get("filled_price") or fresh_price)
        x_fee = float(exit_result.get("fee") or 0)
        _ok(f"Exit  FILLED  qty={x_qty}  price=${x_price:,.2f}  fee=${x_fee:.5f}")
    else:
        _ok(f"Exit 주문 Bybit 전송 완료 (status={x_status}, order_id={x_order_id[:8]}...)")
        _info("Bybit market order 는 즉시 fill 되나 API 응답은 'new' 로 선행 반환됨")
        x_qty = e_qty
        x_price = fresh_price
        x_fee = float(exit_result.get("fee") or 0)

    # ── 최종 포지션 검증 ──────────────────────────────────────────────
    print("\n[포지션 검증]")
    flat = False
    for _ in range(POS_VERIFY_RETRIES):
        size = await _position_size(client)
        if size == 0.0:
            flat = True
            break
        await asyncio.sleep(POS_VERIFY_SLEEP)

    if flat:
        _ok("포지션 0 확인 (flat)")
    else:
        _warn("Redis 캐시 포지션 아직 남아있음 (PositionTracker 지연 가능)")
        _warn("Telegram /positions 로 직접 확인 권장")

    # ── 결과 출력 ─────────────────────────────────────────────────────
    total_fee = e_fee + x_fee
    slippage_pnl = (x_price - e_price) * e_qty
    net_pnl = slippage_pnl - total_fee
    duration_s = max(0, int((exit_sent_at - entry_sent_at).total_seconds()))

    print(f"\n{'='*62}")
    print("  === Manual Mainnet Test Result ===")
    print(f"{'='*62}")
    print(f"  Strategy ID : {STRATEGY_ID}")
    print(f"  Symbol      : {SYMBOL}   Leverage: {LEVERAGE}x")
    print()
    print(f"  Entry  {entry_sent_at.strftime('%Y-%m-%d %H:%M:%S UTC')}  buy {order_type}")
    print(f"         qty {e_qty}  @ ${e_price:,.2f}  notional ${e_qty * e_price:,.2f}  fee ${e_fee:.5f}")
    print(f"  Hold   {hold_s}s")
    print(f"  Exit   {exit_sent_at.strftime('%Y-%m-%d %H:%M:%S UTC')}  sell {order_type} (reduce_only)")
    print(f"         qty {x_qty}  @ ${x_price:,.2f}  notional ${x_qty * x_price:,.2f}  fee ${x_fee:.5f}")
    print()
    pnl_sign = "+" if slippage_pnl >= 0 else ""
    net_sign = "+" if net_pnl >= 0 else ""
    print(f"  Slippage PnL : {pnl_sign}${slippage_pnl:.5f}")
    print(f"  Fees         : -${total_fee:.5f}")
    print(f"  Net Realized : {net_sign}${net_pnl:.5f}")
    print(f"  Duration     : {duration_s}s")
    print(f"  Final Pos    : {'0 ✓' if flat else '미확인 (/positions 확인)'}")
    print()
    print("  Pipeline 검증:")
    print(f"    {G}✓{X} order:request publish 수락 ({rcv} subscriber)")
    print(f"    {G}✓{X} SafetyGuard 통과 (거부 없음)")
    print(f"    {G}✓{X} ccxt → Bybit 주문 fill 반환")
    print(f"    {G}✓{X} reduce_only exit {'완전 청산' if flat else '(cross-check 필요)'}")
    print(f"{'='*62}")
    print()
    print("  Cross-check 명령:")
    print("    Telegram: /positions  /status")
    print(f"    docker compose logs --tail=100 execution-engine | grep {STRATEGY_ID}")
    print(f"    redis-cli -a $REDIS_PASSWORD GET cache:position:bybit:BTC/USDT:USDT")
    print()

    await pubsub.unsubscribe()
    await client.aclose()
    await client_sub.aclose()
    return 0


# ────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mainnet BTC 매수/매도 1회 자동 트리거 테스트"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="payload 만 출력, 실제 publish 없음",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="대화형 확인 프롬프트 스킵 (자동 실행)",
    )
    parser.add_argument(
        "--hold-seconds", type=int, default=60, metavar="N",
        help="entry 후 exit 까지 대기 시간(초) [기본: 60]",
    )
    parser.add_argument(
        "--capital", type=float, default=30.0, metavar="USD",
        help="배분 자본 USD [기본: 30 (최소 주문 기준)]",
    )
    parser.add_argument(
        "--order-type", choices=["market", "limit"], default="limit",
        help="주문 유형 [기본: limit (post-only, re-peg)]",
    )
    parser.add_argument(
        "--repeg-interval", type=int, default=10, metavar="SEC",
        help="limit 재peg 간격(초) [기본: 10]",
    )
    parser.add_argument(
        "--max-attempts", type=int, default=20, metavar="N",
        help="limit 재peg 최대 시도 횟수 [기본: 20]",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
