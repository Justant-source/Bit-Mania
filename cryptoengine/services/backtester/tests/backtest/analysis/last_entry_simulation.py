"""analysis/last_entry_simulation.py
현재 production 전략 파라미터로 Bybit 실제 역사 데이터를 시뮬레이션하여
마지막 진입/청산 시점을 찾습니다.

실행:
    docker compose --profile backtest run --rm backtester \
      python tests/backtest/analysis/last_entry_simulation.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone, timedelta

import aiohttp
import pandas as pd


# ── 현재 production 파라미터 ──────────────────────────────────────────────────
# strategy.py: MIN_FUNDING_RATE = 0.00005 (no top-level min_funding_rate in yaml)
MIN_FUNDING_RATE_PROD  = 0.00005   # 0.005%/8h ≈ 연 2.19%
# yaml: entry.consecutive_intervals: 3
CONSECUTIVE_INTERVALS  = 3
# yaml: exit: min_funding_rate_annualized: 5.0 → 5%/1095 ≈ 0.0000457/8h
EXIT_MIN_RATE          = 0.0000457
# yaml: exit.max_holding_hours: 720 = 90 days in 8h bars
MAX_HOLD_BARS          = 720 // 8  # 90 bars (8h 기준)
# yaml: exit: exit_on_rate_flip: true (3회 연속 역전 → 청산)
EXIT_ON_FLIP_CNT       = 3

# 비교용: backtester FAEngine default (백테스트 결과에 사용된 파라미터)
MIN_FUNDING_RATE_BT    = 0.0001    # 0.01%/8h ≈ 연 4.38%

SYMBOL   = "BTCUSDT"
CATEGORY = "linear"  # Bybit perp


# ── Bybit API: 역사 펀딩비 조회 ───────────────────────────────────────────────

async def fetch_bybit_funding_history(
    symbol: str,
    start_ts: int,
    end_ts: int,
) -> pd.DataFrame:
    """Bybit v5 publicAPI에서 역사 펀딩비를 최대 200개씩 페이지네이션하여 가져옵니다."""
    url = "https://api.bybit.com/v5/market/funding/history"
    records: list[dict] = []

    cur_end = end_ts
    session_timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            params = {
                "category": CATEGORY,
                "symbol":   symbol,
                "startTime": str(start_ts),
                "endTime":   str(cur_end),
                "limit":    "200",
            }
            async with session.get(url, params=params) as resp:
                data = await resp.json()

            result = data.get("result", {})
            rows = result.get("list", [])
            if not rows:
                break

            for r in rows:
                records.append({
                    "ts":   int(r["fundingRateTimestamp"]),
                    "rate": float(r["fundingRate"]),
                })

            # 다음 페이지: 현재 배치 최솟값 - 1ms
            oldest_ts = min(int(r["fundingRateTimestamp"]) for r in rows)
            if oldest_ts <= start_ts:
                break
            cur_end = oldest_ts - 1

    if not records:
        return pd.DataFrame(columns=["ts", "rate"])

    df = pd.DataFrame(records)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.sort_values("ts", inplace=True)
    df.drop_duplicates("ts", inplace=True)
    df.set_index("ts", inplace=True)
    return df


# ── 시뮬레이션 ────────────────────────────────────────────────────────────────

def simulate(
    funding: pd.DataFrame,
    min_rate: float,
    consec_required: int,
    exit_min_rate: float,
    max_hold_bars: int,
    exit_flip_cnt: int,
    label: str,
) -> list[dict]:
    """현재 전략 로직 시뮬레이션. 모든 진입/청산 이벤트를 반환합니다."""
    trades = []
    consec = 0
    position = None

    for ts, row in funding.iterrows():
        rate = float(row["rate"])

        if position is None:
            # 진입 판단
            if rate >= min_rate:
                consec += 1
            else:
                consec = 0

            if consec >= consec_required:
                position = {
                    "entry_ts":     ts,
                    "entry_rate":   rate,
                    "bars_held":    0,
                    "flip_cnt":     0,
                    "funding_acc":  0.0,
                }
                consec = 0
        else:
            # 펀딩비 수취
            position["funding_acc"] += rate
            position["bars_held"]   += 1

            # 청산 판단
            reason = None

            # 1) 역전 연속 3회
            if rate < 0:
                position["flip_cnt"] += 1
            else:
                position["flip_cnt"] = 0

            if position["flip_cnt"] >= exit_flip_cnt:
                reason = "rate_flip"

            # 2) 펀딩비 임계값 이하
            elif rate < exit_min_rate and rate >= 0:
                reason = "rate_below_exit"

            # 3) 최대 보유 기간
            elif position["bars_held"] >= max_hold_bars:
                reason = "max_hold"

            if reason:
                trades.append({
                    "label":       label,
                    "entry_ts":    position["entry_ts"],
                    "exit_ts":     ts,
                    "entry_rate":  position["entry_rate"],
                    "exit_rate":   rate,
                    "bars_held":   position["bars_held"],
                    "funding_acc": round(position["funding_acc"] * 100, 4),
                    "exit_reason": reason,
                })
                position = None

    # 마지막 열린 포지션 기록
    if position:
        last_ts = funding.index[-1]
        trades.append({
            "label":       label,
            "entry_ts":    position["entry_ts"],
            "exit_ts":     None,
            "entry_rate":  position["entry_rate"],
            "exit_rate":   None,
            "bars_held":   position["bars_held"],
            "funding_acc": round(position["funding_acc"] * 100, 4),
            "exit_reason": "OPEN (현재 보유 중)",
        })

    return trades


# ── 출력 헬퍼 ─────────────────────────────────────────────────────────────────

def print_trades(trades: list[dict], title: str) -> None:
    kst = timezone(timedelta(hours=9))
    print(f"\n{'='*70}")
    print(f" {title}")
    print(f"{'='*70}")
    if not trades:
        print("  (진입 없음)")
        return

    for t in trades:
        entry_kst = t["entry_ts"].astimezone(kst).strftime("%Y-%m-%d %H:%M KST")
        if t["exit_ts"] is not None:
            exit_kst = t["exit_ts"].astimezone(kst).strftime("%Y-%m-%d %H:%M KST")
        else:
            exit_kst = "현재 보유 중"
        hold_h = t["bars_held"] * 8
        print(
            f"  진입: {entry_kst}  (rate={t['entry_rate']:.6f})\n"
            f"  청산: {exit_kst}  (rate={t['exit_rate'] if t['exit_rate'] is not None else 'N/A':.6f})\n"
            f"  보유: {hold_h}h ({t['bars_held']}bars)  "
            f"펀딩수취: {t['funding_acc']:.4f}%  "
            f"사유: {t['exit_reason']}\n"
        )

    last = trades[-1]
    entry_kst = last["entry_ts"].astimezone(kst).strftime("%Y-%m-%d %H:%M KST")
    print(f"  ★ 마지막 진입: {entry_kst}  (exit_reason: {last['exit_reason']})")


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    now     = datetime.now(timezone.utc)
    # 최근 1년 데이터 (Bybit는 최대 2년 제공)
    start   = now - timedelta(days=365)
    end_ts  = int(now.timestamp() * 1000)
    start_ts = int(start.timestamp() * 1000)

    print(f"Bybit BTCUSDT 역사 펀딩비 조회 중 ({start.date()} ~ {now.date()})...")
    funding = await fetch_bybit_funding_history(SYMBOL, start_ts, end_ts)

    if funding.empty:
        print("ERROR: 데이터 없음")
        sys.exit(1)

    print(f"  → {len(funding)}개 레코드 수신 ({funding.index[0].date()} ~ {funding.index[-1].date()})")
    print(f"  → 최근 10개 펀딩비:")
    kst = timezone(timedelta(hours=9))
    for ts, row in funding.tail(10).iterrows():
        rate_ann = float(row["rate"]) * 3 * 365 * 100
        print(f"     {ts.astimezone(kst).strftime('%Y-%m-%d %H:%M KST')}  "
              f"rate={float(row['rate']):.6f}  ({rate_ann:.2f}% ann)")

    # ── 시뮬레이션 1: 현재 production 파라미터
    prod_trades = simulate(
        funding, MIN_FUNDING_RATE_PROD, CONSECUTIVE_INTERVALS,
        EXIT_MIN_RATE, MAX_HOLD_BARS, EXIT_ON_FLIP_CNT,
        label="PROD",
    )
    print_trades(prod_trades, f"[PROD] min_rate={MIN_FUNDING_RATE_PROD} ({MIN_FUNDING_RATE_PROD*3*365*100:.2f}% ann) / consecutive={CONSECUTIVE_INTERVALS}")

    # ── 시뮬레이션 2: 백테스터 default 파라미터 (0.01%)
    bt_trades = simulate(
        funding, MIN_FUNDING_RATE_BT, CONSECUTIVE_INTERVALS,
        EXIT_MIN_RATE, MAX_HOLD_BARS, EXIT_ON_FLIP_CNT,
        label="BT_DEFAULT",
    )
    print_trades(bt_trades, f"[BT_DEFAULT] min_rate={MIN_FUNDING_RATE_BT} ({MIN_FUNDING_RATE_BT*3*365*100:.2f}% ann) / consecutive={CONSECUTIVE_INTERVALS}")

    # ── 시뮬레이션 3: Phase5 기준 (연 25%, 4회 연속)
    p5_rate = 0.00012  # yaml phase5.entry.min_funding_rate
    p5_trades = simulate(
        funding, p5_rate, 4,
        EXIT_MIN_RATE, MAX_HOLD_BARS, EXIT_ON_FLIP_CNT,
        label="PHASE5",
    )
    print_trades(p5_trades, f"[PHASE5] min_rate={p5_rate} ({p5_rate*3*365*100:.2f}% ann) / consecutive=4")

    # ── 요약
    print(f"\n{'='*70}")
    print(" 요약")
    print(f"{'='*70}")
    def last_entry_str(trades):
        if not trades:
            return "없음 (1년 내 진입 기록 없음)"
        t = trades[-1]
        entry_kst = t["entry_ts"].astimezone(kst).strftime("%Y-%m-%d %H:%M KST")
        if t["exit_ts"]:
            exit_kst = t["exit_ts"].astimezone(kst).strftime("%Y-%m-%d %H:%M KST")
            return f"진입 {entry_kst} → 청산 {exit_kst} ({t['exit_reason']})"
        else:
            return f"진입 {entry_kst} → 현재 보유 중"

    print(f"  PROD      총 {len(prod_trades)}회 | 마지막: {last_entry_str(prod_trades)}")
    print(f"  BT_DEFAULT 총 {len(bt_trades)}회 | 마지막: {last_entry_str(bt_trades)}")
    print(f"  PHASE5    총 {len(p5_trades)}회 | 마지막: {last_entry_str(p5_trades)}")


if __name__ == "__main__":
    asyncio.run(main())
