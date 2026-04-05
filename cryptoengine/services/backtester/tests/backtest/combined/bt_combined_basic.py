"""bt_combined.py — 복합 전략 백테스트 (Orchestrator 시뮬레이션).

전략:
  - 펀딩비 차익거래 + 그리드 전략 조합
  - 레짐 감지 (ATR 기반):
      trending  → funding-arb 70%  / grid 30%
      ranging   → funding-arb 30%  / grid 70%
      volatile  → funding-arb 50%  / grid 50%  (+ 포지션 축소 50%)
  - 초기 자본 10,000 USDT
  - 파산 방지 (자본 <= 0 시 중단)

레짐 감지 기준 (1h ATR/price 비율):
  - volatile : atr_ratio > 0.025  (2.5% 이상)
  - trending : atr_ratio > 0.012  (1.2% ~ 2.5%)
  - ranging  : atr_ratio <= 0.012 (1.2% 이하)

펀딩비 전략 모델 (델타 뉴트럴):
  - 선물 숏 + 현물 롱 동일 노셔널 → 가격 변동 상쇄, 펀딩비만 수취
  - position_notional = capital * fa_alloc_ratio (방향성 숏 PnL 없음)

그리드 전략:
  - EMA(20) 추세 필터: 상승 추세 → 롱 그리드만, 하락 추세 → 숏 그리드만
  - order_size = capital * CAPITAL_PER_GRID (동적 포지션 크기)

결과:
  - /app/results/bt_combined.json
  - PostgreSQL backtest_results 테이블

실행:
    python tests/backtest/bt_combined.py \
        --start 2025-10-01 --end 2026-04-01 --symbol BTCUSDT
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import asyncpg
import numpy as np
import pandas as pd

# ── 상수 ───────────────────────────────────────────────────────────────────────

INITIAL_CAPITAL = 10_000.0
TRADING_FEE = 0.00055
ATR_PERIOD = 14
EMA_PERIOD = 20              # 추세 필터용 EMA 기간

# 레짐 임계값
VOLATILE_THRESHOLD = 0.025
TRENDING_THRESHOLD = 0.012

# 레짐별 가중치 (funding_arb : grid)
REGIME_WEIGHTS: dict[str, tuple[float, float]] = {
    "trending": (0.70, 0.30),
    "ranging": (0.30, 0.70),
    "volatile": (0.50, 0.50),
}
# volatile 레짐에서 포지션 크기 축소 비율
VOLATILE_SCALE = 0.5

# 펀딩비 진입/청산 임계값
FUNDING_ENTRY = 0.0001   # 0.01%
FUNDING_EXIT = 0.00005   # 0.005%

# 그리드 설정
GRID_COUNT = 10
ATR_MULTIPLIER = 0.5
CAPITAL_PER_GRID = 0.03      # 현재 자본의 3% per grid
TREND_NEUTRAL_BAND = 0.005   # ±0.5% 이내 중립 구간

RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "/app/results"))
DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CREATE_BACKTEST_RESULTS = """
CREATE TABLE IF NOT EXISTS backtest_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    strategy        TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    start_date      TIMESTAMPTZ NOT NULL,
    end_date        TIMESTAMPTZ NOT NULL,
    initial_capital DOUBLE PRECISION NOT NULL,
    final_equity    DOUBLE PRECISION NOT NULL,
    total_return    DOUBLE PRECISION NOT NULL,
    sharpe_ratio    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION,
    win_rate        DOUBLE PRECISION,
    total_trades    INTEGER,
    metadata        JSONB
);
"""

Regime = Literal["trending", "ranging", "volatile"]


# ── 데이터 로드 ────────────────────────────────────────────────────────────────

async def _load_ohlcv(
    pool: asyncpg.Pool, symbol: str, start: datetime, end: datetime
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv_history
            WHERE exchange = 'bybit' AND symbol = $1 AND timeframe = '1h'
              AND timestamp >= $2 AND timestamp < $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


async def _load_funding(
    pool: asyncpg.Pool, symbol: str, start: datetime, end: datetime
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, rate
            FROM funding_rate_history
            WHERE exchange = 'bybit' AND symbol = $1
              AND timestamp >= $2 AND timestamp < $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


# ── ATR / EMA / 레짐 계산 ─────────────────────────────────────────────────────

def _calc_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _calc_ema(df: pd.DataFrame, period: int = EMA_PERIOD) -> pd.Series:
    return df["close"].ewm(span=period, adjust=False).mean()


def _detect_regime(atr: float, price: float) -> Regime:
    if price <= 0:
        return "ranging"
    ratio = atr / price
    if ratio > VOLATILE_THRESHOLD:
        return "volatile"
    elif ratio > TRENDING_THRESHOLD:
        return "trending"
    else:
        return "ranging"


def _detect_trend(price: float, ema: float) -> str:
    """추세 감지: 'up' / 'down' / 'neutral'."""
    if ema <= 0:
        return "neutral"
    ratio = (price - ema) / ema
    if ratio > TREND_NEUTRAL_BAND:
        return "up"
    elif ratio < -TREND_NEUTRAL_BAND:
        return "down"
    else:
        return "neutral"


# ── 복합 전략 백테스트 엔진 ────────────────────────────────────────────────────

def run_backtest(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """
    복합 전략 백테스트.
    레짐에 따라 두 전략의 가중치를 동적으로 조정.

    펀딩비 전략: 델타 뉴트럴 (가격 PnL 없음, 펀딩비만 수취)
    그리드 전략: EMA 추세 필터 + 동적 포지션 크기 + 파산 방지
    """
    df = ohlcv.copy()
    df["atr"] = _calc_atr(df)
    df["ema"] = _calc_ema(df)

    # 펀딩비 정렬
    if not funding.empty:
        df["funding_rate"] = funding["rate"].reindex(df.index, method="ffill")
    else:
        df["funding_rate"] = 0.0

    capital = INITIAL_CAPITAL
    equity_curve: list[float] = [capital]
    returns: list[float] = []

    # 펀딩비 전략 상태 (델타 뉴트럴)
    fa_in_position = False
    fa_position_notional = 0.0   # 선물 숏 = 현물 롱 동일 노셔널
    fa_entry_funding_sum = 0.0   # 진입 이후 누적 펀딩

    # 그리드 전략 상태
    grid_orders: dict[float, dict] = {}
    grid_base_price = 0.0
    grid_spacing = 0.0
    grid_trend = "neutral"

    # 통계
    fa_trades: list[dict] = []
    grid_trades: list[dict] = []
    grid_wins = 0
    fa_wins = 0
    regime_counts: dict[str, int] = {"trending": 0, "ranging": 0, "volatile": 0}

    warmup = max(ATR_PERIOD, EMA_PERIOD) + 1

    for i, (ts, row) in enumerate(df.iterrows()):
        price = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])
        atr = float(row["atr"]) if not math.isnan(row["atr"]) else 0.0
        ema = float(row["ema"]) if not math.isnan(row["ema"]) else 0.0
        rate = float(row["funding_rate"]) if not math.isnan(row["funding_rate"]) else 0.0
        prev_equity = capital

        if i < warmup or atr == 0 or ema == 0:
            equity_curve.append(capital)
            returns.append(0.0)
            continue

        # 파산 방지
        if capital <= 0:
            capital = 0.0
            break

        # 레짐 감지
        regime: Regime = _detect_regime(atr, price)
        regime_counts[regime] += 1

        fa_weight, grid_weight = REGIME_WEIGHTS[regime]
        position_scale = VOLATILE_SCALE if regime == "volatile" else 1.0

        # ── 펀딩비 전략 (델타 뉴트럴) ────────────────────────────────────────
        # 노셔널 = 현재 자본 * 펀딩비 가중치 * 포지션 스케일
        # 가격 PnL 없음 — 선물 숏과 현물 롱이 상쇄

        if not fa_in_position:
            if rate > FUNDING_ENTRY and capital > 0:
                fa_position_notional = capital * fa_weight * position_scale
                fee = fa_position_notional * TRADING_FEE * 2  # 선물 + 현물
                capital -= fee
                fa_entry_funding_sum = 0.0
                fa_in_position = True
        else:
            # 펀딩비 수취 (8h 주기를 1h 단위로 근사: rate/8 per hour)
            funding_pnl = fa_position_notional * (rate / 8)
            capital += funding_pnl
            fa_entry_funding_sum += funding_pnl

            if rate < FUNDING_EXIT:
                fee = fa_position_notional * TRADING_FEE * 2  # 선물 청산 + 현물 매도
                capital -= fee
                trade_pnl = fa_entry_funding_sum - fee
                if trade_pnl > 0:
                    fa_wins += 1
                fa_trades.append({
                    "entry_ts": str(ts),
                    "funding_collected": round(fa_entry_funding_sum, 4),
                    "exit_rate": rate,
                })
                fa_in_position = False
                fa_position_notional = 0.0
                fa_entry_funding_sum = 0.0

            # 파산 방지 (펀딩비 처리 후 재확인)
            if capital <= 0:
                capital = 0.0
                fa_in_position = False
                break

        # ── 그리드 전략 (추세 필터 + 동적 포지션 크기) ────────────────────────
        trend = _detect_trend(price, ema)

        # 그리드 초기화 / 재설정 판단
        should_reset = False
        if not grid_orders:
            should_reset = True
        else:
            half_range = grid_spacing * (GRID_COUNT // 2)
            if half_range > 0 and abs(price - grid_base_price) > half_range * 1.5:
                should_reset = True
            elif trend != "neutral" and grid_trend != "neutral" and trend != grid_trend:
                should_reset = True  # 추세 반전 시 재설정

        if should_reset and atr > 0:
            # 기존 그리드 정리
            for level_price, order in list(grid_orders.items()):
                if order["active"]:
                    fee = order["qty"] * price * TRADING_FEE
                    pnl = (
                        (price - order["entry_price"]) * order["qty"]
                        if order["side"] == "buy"
                        else (order["entry_price"] - price) * order["qty"]
                    )
                    capital += pnl - fee
            grid_orders.clear()

            # 파산 방지 재확인
            if capital <= 0:
                capital = 0.0
                break

            grid_trend = trend
            grid_base_price = price
            grid_spacing = atr * ATR_MULTIPLIER
            half = GRID_COUNT // 2

            for k in range(-half, half + 1):
                if k == 0:
                    continue
                level = price + k * grid_spacing
                if level <= 0:
                    continue

                side = "buy" if k < 0 else "sell"
                # 추세 필터: 상승 → 롱 그리드만, 하락 → 숏 그리드만
                if trend == "up" and side == "sell":
                    continue
                if trend == "down" and side == "buy":
                    continue

                # 동적 포지션 크기 (그리드 가중치 반영)
                order_size = capital * CAPITAL_PER_GRID * grid_weight * position_scale
                qty = order_size / level
                grid_orders[round(level, 2)] = {
                    "side": side,
                    "qty": qty,
                    "active": False,
                    "entry_price": level,
                }

        # 그리드 체결
        for level_price, order in list(grid_orders.items()):
            if order["active"]:
                target = (
                    level_price + grid_spacing
                    if order["side"] == "buy"
                    else level_price - grid_spacing
                )
                if order["side"] == "buy" and high >= target:
                    pnl = (target - order["entry_price"]) * order["qty"]
                    fee = order["qty"] * target * TRADING_FEE * 2
                    capital += pnl - fee
                    if pnl > 0:
                        grid_wins += 1
                    grid_trades.append({"entry": order["entry_price"], "exit": target, "pnl": pnl, "side": "buy"})
                    order["active"] = False
                elif order["side"] == "sell" and low <= target:
                    pnl = (order["entry_price"] - target) * order["qty"]
                    fee = order["qty"] * target * TRADING_FEE * 2
                    capital += pnl - fee
                    if pnl > 0:
                        grid_wins += 1
                    grid_trades.append({"entry": order["entry_price"], "exit": target, "pnl": pnl, "side": "sell"})
                    order["active"] = False
            else:
                if order["side"] == "buy" and low <= level_price <= high:
                    capital -= order["qty"] * level_price * TRADING_FEE
                    order["active"] = True
                elif order["side"] == "sell" and low <= level_price <= high:
                    capital -= order["qty"] * level_price * TRADING_FEE
                    order["active"] = True

        # 파산 방지 (그리드 체결 후 재확인)
        if capital <= 0:
            capital = 0.0
            equity_curve.append(capital)
            returns.append(-1.0 if prev_equity > 0 else 0.0)
            break

        equity_curve.append(capital)
        hourly_return = (capital - prev_equity) / prev_equity if prev_equity > 0 else 0.0
        returns.append(hourly_return)

    # 미청산 정리
    if not df.empty:
        last_price = float(df.iloc[-1]["close"])
        # 펀딩비 포지션 청산 (델타 뉴트럴: 가격 PnL 없음)
        if fa_in_position and fa_position_notional > 0 and capital > 0:
            fee = fa_position_notional * TRADING_FEE * 2
            capital -= fee
        # 그리드 포지션 청산
        for level_price, order in grid_orders.items():
            if order["active"]:
                pnl = (
                    (last_price - order["entry_price"]) * order["qty"]
                    if order["side"] == "buy"
                    else (order["entry_price"] - last_price) * order["qty"]
                )
                capital += pnl - order["qty"] * last_price * TRADING_FEE
        if equity_curve:
            equity_curve[-1] = capital

    equity_series = pd.Series(equity_curve)
    returns_series = pd.Series(returns)

    total_trades = len(fa_trades) + len(grid_trades)
    wins = fa_wins + grid_wins
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_return = (capital - INITIAL_CAPITAL) / INITIAL_CAPITAL

    std = returns_series.std()
    sharpe = float((returns_series.mean() / std) * math.sqrt(8760)) if std > 0 else 0.0
    roll_max = equity_series.cummax()
    mdd = float(((equity_series - roll_max) / roll_max).min())

    return {
        "strategy": "combined",
        "initial_capital": INITIAL_CAPITAL,
        "final_equity": round(capital, 4),
        "total_return_pct": round(total_return * 100, 4),
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown_pct": round(mdd * 100, 4),
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate * 100, 2),
        "funding_arb_trades": len(fa_trades),
        "grid_trades": len(grid_trades),
        "regime_distribution": regime_counts,
        "regime_weights": REGIME_WEIGHTS,
        "fa_trades_sample": fa_trades[:5],
        "grid_trades_sample": grid_trades[:5],
    }


# ── 결과 저장 ──────────────────────────────────────────────────────────────────

def _save_json(result: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"[INFO] JSON 결과 저장: {path}")


async def _save_to_db(
    pool: asyncpg.Pool,
    result: dict,
    symbol: str,
    start: datetime,
    end: datetime,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_BACKTEST_RESULTS)
        await conn.execute(
            """
            INSERT INTO backtest_results
                (strategy, symbol, start_date, end_date, initial_capital,
                 final_equity, total_return, sharpe_ratio, max_drawdown,
                 win_rate, total_trades, metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
            """,
            result["strategy"],
            symbol,
            start,
            end,
            result["initial_capital"],
            result["final_equity"],
            result["total_return_pct"],
            result["sharpe_ratio"],
            result["max_drawdown_pct"],
            result["win_rate_pct"],
            result["total_trades"],
            json.dumps(result),
        )
    print("[INFO] DB backtest_results 저장 완료")


# ── 진입점 ─────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    print("[INFO] DB 연결 중...")
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    print(f"[INFO] 데이터 로드 중... ({args.symbol}, {args.start} ~ {args.end})")
    ohlcv = await _load_ohlcv(pool, args.symbol, start, end)
    funding = await _load_funding(pool, args.symbol, start, end)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    print(f"[INFO] OHLCV {len(ohlcv)}행, 펀딩비 {len(funding)}행 로드 완료")
    if funding.empty:
        print("[WARN] 펀딩비 데이터 없음. 펀딩비 수익 없이 백테스트 진행.")

    print("[INFO] 복합 전략 백테스트 실행 중...")
    result = run_backtest(ohlcv, funding)

    print("\n=== 복합 전략 백테스트 결과 ===")
    print(f"  초기 자본          : {result['initial_capital']:,.2f} USDT")
    print(f"  최종 자산          : {result['final_equity']:,.4f} USDT")
    print(f"  총 수익률          : {result['total_return_pct']:.4f}%")
    print(f"  Sharpe Ratio       : {result['sharpe_ratio']:.4f}")
    print(f"  Max Drawdown       : {result['max_drawdown_pct']:.4f}%")
    print(f"  총 거래 수         : {result['total_trades']}")
    print(f"  승률               : {result['win_rate_pct']:.2f}%")
    print(f"  펀딩비 전략 거래   : {result['funding_arb_trades']}")
    print(f"  그리드 전략 거래   : {result['grid_trades']}")
    print(f"  레짐 분포          : {result['regime_distribution']}")

    out_path = RESULTS_DIR / "bt_combined.json"
    _save_json(result, out_path)

    # DB 저장
    await _save_to_db(pool, result, args.symbol, start, end)
    await pool.close()

    print("\n[DONE] 복합 전략 백테스트 완료")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="복합 전략 백테스트 (Orchestrator 시뮬레이션)")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
