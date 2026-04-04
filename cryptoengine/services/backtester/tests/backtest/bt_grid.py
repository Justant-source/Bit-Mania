"""bt_grid.py — ATR 기반 그리드 전략 백테스트.

전략:
  - 1h OHLCV 데이터 사용
  - ATR(14)을 기반으로 그리드 간격 설정
  - EMA(20) 추세 필터: 상승 추세 → 롱 그리드만, 하락 추세 → 숏 그리드만
  - 10개 그리드 레벨 (추세 방향으로 운용)
  - 포지션 크기: 현재 자본의 3% per grid (동적 조정)
  - 초기 자본 10,000 USDT
  - 파산 방지 (자본 <= 0 시 중단)

결과:
  - /app/results/bt_grid.json
  - PostgreSQL backtest_results 테이블

실행:
    python tests/backtest/bt_grid.py \
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

import asyncpg
import numpy as np
import pandas as pd

# ── 상수 ───────────────────────────────────────────────────────────────────────

INITIAL_CAPITAL = 10_000.0
CAPITAL_PER_GRID = 0.03      # 현재 자본의 3% per grid
GRID_COUNT = 10              # 그리드 총 개수
ATR_PERIOD = 14
EMA_PERIOD = 20              # 추세 필터용 EMA 기간
ATR_MULTIPLIER = 0.5         # 그리드 간격 = ATR * 멀티플라이어
TREND_NEUTRAL_BAND = 0.005   # ±0.5% 이내 중립 구간
TRADING_FEE = 0.00055        # Bybit taker fee
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


# ── 데이터 로드 ────────────────────────────────────────────────────────────────

async def _load_ohlcv(
    pool: asyncpg.Pool, symbol: str, start: datetime, end: datetime,
    timeframe: str = "1h",
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv_history
            WHERE exchange = 'bybit' AND symbol = $1 AND timeframe = $4
              AND timestamp >= $2 AND timestamp < $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end, timeframe,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


# ── 지표 계산 ──────────────────────────────────────────────────────────────────

def _calc_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _calc_ema(df: pd.DataFrame, period: int = EMA_PERIOD) -> pd.Series:
    return df["close"].ewm(span=period, adjust=False).mean()


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


# ── 그리드 백테스트 엔진 ───────────────────────────────────────────────────────

def run_backtest(ohlcv: pd.DataFrame) -> dict:
    """ATR 기반 그리드 전략 백테스트 (추세 필터 + 동적 포지션 크기 + 파산 방지)."""
    df = ohlcv.copy()
    df["atr"] = _calc_atr(df)
    df["ema"] = _calc_ema(df)

    capital = INITIAL_CAPITAL
    equity_curve: list[float] = [capital]
    returns: list[float] = []

    # 그리드 상태
    grid_orders: dict[float, dict] = {}
    grid_base_price = 0.0
    grid_spacing = 0.0
    grid_trend = "neutral"  # 현재 그리드가 설정된 당시 추세

    trades: list[dict] = []
    wins = 0

    # ATR + EMA 모두 계산되는 시점부터 시작
    warmup = max(ATR_PERIOD, EMA_PERIOD) + 1

    for i, (ts, row) in enumerate(df.iterrows()):
        price = float(row["close"])
        atr = float(row["atr"]) if not math.isnan(row["atr"]) else 0.0
        ema = float(row["ema"]) if not math.isnan(row["ema"]) else 0.0
        prev_equity = capital

        if i < warmup or atr == 0 or ema == 0:
            equity_curve.append(capital)
            returns.append(0.0)
            continue

        # 파산 방지
        if capital <= 0:
            break

        trend = _detect_trend(price, ema)

        # 그리드 초기화 또는 재설정 판단
        should_reset = False
        if not grid_orders:
            should_reset = True
        else:
            # 가격이 그리드 범위를 벗어나거나 추세가 반전된 경우 재설정
            half_range = grid_spacing * (GRID_COUNT // 2)
            if abs(price - grid_base_price) > half_range * 1.5:
                should_reset = True
            # 추세가 완전히 반전된 경우 재설정 (중립 제외)
            elif trend != "neutral" and grid_trend != "neutral" and trend != grid_trend:
                should_reset = True

        if should_reset:
            # 기존 그리드 청산
            if grid_orders:
                for level_price, order in list(grid_orders.items()):
                    if order["active"]:
                        fee = order["qty"] * price * TRADING_FEE
                        pnl = (
                            (price - level_price) * order["qty"]
                            if order["side"] == "buy"
                            else (level_price - price) * order["qty"]
                        )
                        capital += pnl - fee
                grid_orders.clear()

            # 파산 방지 재확인
            if capital <= 0:
                capital = 0.0
                break

            # 추세 필터에 따른 그리드 방향 결정
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

                # 추세 방향에 따라 그리드 필터링
                side = "buy" if k < 0 else "sell"
                if trend == "up" and side == "sell":
                    continue   # 상승 추세: 롱(매수) 그리드만 운용
                if trend == "down" and side == "buy":
                    continue   # 하락 추세: 숏(매도) 그리드만 운용
                # neutral: 양방향 모두 운용

                # 동적 포지션 크기: 현재 자본 기준
                order_size = capital * CAPITAL_PER_GRID
                qty = order_size / level
                grid_orders[round(level, 2)] = {
                    "side": side,
                    "qty": qty,
                    "active": False,
                    "entry_price": level,
                }

        # 그리드 주문 체결 확인
        high = float(row["high"])
        low = float(row["low"])

        for level_price, order in list(grid_orders.items()):
            if order["active"]:
                # 활성 포지션: 반대편 그리드에서 청산
                target = (
                    level_price + grid_spacing
                    if order["side"] == "buy"
                    else level_price - grid_spacing
                )
                if order["side"] == "buy" and high >= target:
                    pnl = (target - order["entry_price"]) * order["qty"]
                    fee = order["qty"] * target * TRADING_FEE * 2
                    capital += pnl - fee
                    wins += 1 if pnl > 0 else 0
                    trades.append({"entry": order["entry_price"], "exit": target, "pnl": pnl, "side": "buy"})
                    order["active"] = False
                elif order["side"] == "sell" and low <= target:
                    pnl = (order["entry_price"] - target) * order["qty"]
                    fee = order["qty"] * target * TRADING_FEE * 2
                    capital += pnl - fee
                    wins += 1 if pnl > 0 else 0
                    trades.append({"entry": order["entry_price"], "exit": target, "pnl": pnl, "side": "sell"})
                    order["active"] = False
            else:
                # 비활성: 진입 체결 확인
                if order["side"] == "buy" and low <= level_price <= high:
                    fee = order["qty"] * level_price * TRADING_FEE
                    capital -= fee
                    order["active"] = True
                elif order["side"] == "sell" and low <= level_price <= high:
                    fee = order["qty"] * level_price * TRADING_FEE
                    capital -= fee
                    order["active"] = True

        # 파산 방지 (체결 후 재확인)
        if capital <= 0:
            capital = 0.0
            equity_curve.append(capital)
            returns.append(-1.0 if prev_equity > 0 else 0.0)
            break

        equity_curve.append(capital)
        hourly_return = (capital - prev_equity) / prev_equity if prev_equity > 0 else 0.0
        returns.append(hourly_return)

    # 미청산 포지션 청산
    if grid_orders and not df.empty:
        last_price = float(df.iloc[-1]["close"])
        for level_price, order in grid_orders.items():
            if order["active"]:
                pnl = (
                    (last_price - order["entry_price"]) * order["qty"]
                    if order["side"] == "buy"
                    else (order["entry_price"] - last_price) * order["qty"]
                )
                fee = order["qty"] * last_price * TRADING_FEE
                capital += pnl - fee
        if equity_curve:
            equity_curve[-1] = capital

    equity_series = pd.Series(equity_curve)
    returns_series = pd.Series(returns)

    total_trades = len(trades)
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_return = (capital - INITIAL_CAPITAL) / INITIAL_CAPITAL

    # Sharpe
    std = returns_series.std()
    sharpe = float((returns_series.mean() / std) * math.sqrt(8760)) if std > 0 else 0.0

    # MDD
    roll_max = equity_series.cummax()
    mdd = float(((equity_series - roll_max) / roll_max).min())

    return {
        "strategy": "grid_trading",
        "initial_capital": INITIAL_CAPITAL,
        "final_equity": round(capital, 4),
        "total_return_pct": round(total_return * 100, 4),
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown_pct": round(mdd * 100, 4),
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate * 100, 2),
        "grid_count": GRID_COUNT,
        "atr_period": ATR_PERIOD,
        "ema_period": EMA_PERIOD,
        "atr_multiplier": ATR_MULTIPLIER,
        "capital_per_grid": CAPITAL_PER_GRID,
        "trades_sample": trades[:10],
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

    print(f"[INFO] 데이터 로드 중... ({args.symbol}, {args.start} ~ {args.end}, {args.timeframe})")
    ohlcv = await _load_ohlcv(pool, args.symbol, start, end, timeframe=args.timeframe)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    print(f"[INFO] OHLCV {len(ohlcv)}행 로드 완료")
    print("[INFO] 그리드 백테스트 실행 중...")

    result = run_backtest(ohlcv)

    print("\n=== 그리드 백테스트 결과 ===")
    print(f"  초기 자본   : {result['initial_capital']:,.2f} USDT")
    print(f"  최종 자산   : {result['final_equity']:,.4f} USDT")
    print(f"  총 수익률   : {result['total_return_pct']:.4f}%")
    print(f"  Sharpe Ratio: {result['sharpe_ratio']:.4f}")
    print(f"  Max Drawdown: {result['max_drawdown_pct']:.4f}%")
    print(f"  총 거래 수  : {result['total_trades']}")
    print(f"  승률        : {result['win_rate_pct']:.2f}%")

    out_path = RESULTS_DIR / "bt_grid.json"
    _save_json(result, out_path)

    # DB 저장
    await _save_to_db(pool, result, args.symbol, start, end)
    await pool.close()

    print("\n[DONE] 그리드 백테스트 완료")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ATR 기반 그리드 전략 백테스트")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--timeframe", default="1h", choices=["15m", "1h", "4h"],
        help="OHLCV 타임프레임 (기본값: 1h)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
