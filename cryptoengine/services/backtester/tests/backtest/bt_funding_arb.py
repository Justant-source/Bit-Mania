"""bt_funding_arb.py — 펀딩비 차익거래 전략 백테스트.

전략:
  - 펀딩비 > 0.01%  → 숏 포지션 진입 (펀딩비 수취)
  - 펀딩비 < 0.005% → 포지션 청산
  - 레버리지 2배, 포지션 크기 총 자본의 50%
  - 초기 자본 10,000 USDT

결과:
  - /app/results/bt_funding_arb.json
  - PostgreSQL daily_reports 테이블

실행:
    python tests/backtest/bt_funding_arb.py \
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
LEVERAGE = 2.0
POSITION_RATIO = 0.50          # 총 자본의 50%
ENTRY_THRESHOLD = 0.0001       # 0.01%
EXIT_THRESHOLD = 0.00005       # 0.005%
SPOT_FEE_RATE = 0.0001         # Bybit 현물 taker 0.01%
PERP_FEE_RATE = 0.00055        # Bybit 선물 taker 0.055%
ENTRY_FEE_RATE = SPOT_FEE_RATE + PERP_FEE_RATE   # 0.00065 (진입 편도)
EXIT_FEE_RATE = SPOT_FEE_RATE + PERP_FEE_RATE    # 0.00065 (청산 편도)
ROUND_TRIP_FEE = ENTRY_FEE_RATE + EXIT_FEE_RATE  # 0.0013 (왕복 총비용)
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


# ── 지표 계산 ──────────────────────────────────────────────────────────────────

def _sharpe_ratio(returns: pd.Series, periods_per_year: int = 8760) -> float:
    """시간 단위 수익률 시리즈에서 연율화 Sharpe 비율 계산."""
    if returns.std() == 0:
        return 0.0
    return float((returns.mean() / returns.std()) * math.sqrt(periods_per_year))


def _max_drawdown(equity: pd.Series) -> float:
    """최대 낙폭(MDD) 계산. 0~1 범위."""
    roll_max = equity.cummax()
    drawdown = (equity - roll_max) / roll_max
    return float(drawdown.min())


# ── 백테스트 엔진 ──────────────────────────────────────────────────────────────

def run_backtest(ohlcv: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """
    델타 뉴트럴 펀딩비 차익 전략 백테스트.

    모델: 선물 숏 + 현물 롱 동일 노셔널 → 가격 변동 상쇄, 펀딩비만 수취.
    - 진입 시 노셔널 = capital * POSITION_RATIO (현물 + 선물 마진 합산)
    - 수익원: funding_pnl = notional * rate_per_period (8시간마다, 1h 근사)
    - 비용: 진입/청산 수수료 (선물 + 현물 각각)
    """
    if funding.empty:
        raise ValueError("펀딩비 데이터가 없습니다. seed_historical.py --data-type funding 먼저 실행하세요.")

    df = ohlcv.copy()
    df["funding_rate"] = funding["rate"].reindex(df.index, method="ffill")

    capital = INITIAL_CAPITAL
    equity_curve: list[float] = [capital]
    returns: list[float] = []

    in_position = False
    position_notional = 0.0   # 선물 숏 = 현물 롱 동일 노셔널
    entry_funding_sum = 0.0   # 진입 시부터 누적 펀딩

    trades: list[dict] = []
    funding_collected = 0
    wins = 0

    for ts, row in df.iterrows():
        rate = float(row["funding_rate"]) if not math.isnan(row["funding_rate"]) else 0.0
        prev_equity = capital

        if not in_position:
            if rate > ENTRY_THRESHOLD and capital > 0:
                # 노셔널 = 현재 자본의 POSITION_RATIO
                # 수수료: 선물 진입 + 현물 매수 (양쪽 모두 taker)
                position_notional = capital * POSITION_RATIO
                fee = position_notional * ENTRY_FEE_RATE * 2  # 선물 + 현물
                capital -= fee
                entry_funding_sum = 0.0
                in_position = True
        else:
            # 펀딩비 수취 (8h 주기를 1h 단위로 근사: rate/8 per hour)
            # 숏 포지션 → 펀딩 수취 (rate > 0 이면 숏 홀더가 수취)
            funding_pnl = position_notional * (rate / 8)
            capital += funding_pnl
            entry_funding_sum += funding_pnl
            if funding_pnl > 0:
                funding_collected += 1

            # 청산 조건
            if rate < EXIT_THRESHOLD:
                fee = position_notional * EXIT_FEE_RATE * 2  # 선물 청산 + 현물 매도
                capital -= fee
                trade_pnl = entry_funding_sum - fee
                if trade_pnl > 0:
                    wins += 1
                trades.append({
                    "entry_ts": str(ts),
                    "funding_collected": round(entry_funding_sum, 4),
                    "exit_rate": rate,
                })
                in_position = False
                position_notional = 0.0
                entry_funding_sum = 0.0

            # 자본이 0 이하면 강제 청산 (파산 방지)
            if capital <= 0:
                capital = 0.0
                in_position = False
                break

        equity_curve.append(capital)
        hourly_return = (capital - prev_equity) / prev_equity if prev_equity > 0 else 0.0
        returns.append(hourly_return)

    # 미청산 포지션 마감
    if in_position and capital > 0:
        fee = position_notional * EXIT_FEE_RATE * 2
        capital -= fee
        if entry_funding_sum > fee:
            wins += 1
        equity_curve[-1] = capital

    equity_series = pd.Series(equity_curve)
    returns_series = pd.Series(returns)

    total_trades = len(trades)
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_return = (capital - INITIAL_CAPITAL) / INITIAL_CAPITAL

    result = {
        "strategy": "funding_arb",
        "initial_capital": INITIAL_CAPITAL,
        "final_equity": round(capital, 4),
        "total_return_pct": round(total_return * 100, 4),
        "sharpe_ratio": round(_sharpe_ratio(returns_series), 4),
        "max_drawdown_pct": round(_max_drawdown(equity_series) * 100, 4),
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate * 100, 2),
        "funding_collected_count": funding_collected,
        "leverage": LEVERAGE,
        "position_ratio": POSITION_RATIO,
        "entry_threshold": ENTRY_THRESHOLD,
        "exit_threshold": EXIT_THRESHOLD,
        "trades_sample": trades[:10],  # 첫 10개 거래만 저장
    }
    return result


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

    print(f"[INFO] DB 연결 중...")
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    print(f"[INFO] 데이터 로드 중... ({args.symbol}, {args.start} ~ {args.end})")
    ohlcv = await _load_ohlcv(pool, args.symbol, start, end)
    funding = await _load_funding(pool, args.symbol, start, end)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    print(f"[INFO] OHLCV {len(ohlcv)}행, 펀딩비 {len(funding)}행 로드 완료")
    print("[INFO] 백테스트 실행 중...")

    result = run_backtest(ohlcv, funding)

    print("\n=== 백테스트 결과 ===")
    print(f"  초기 자본       : {result['initial_capital']:,.2f} USDT")
    print(f"  최종 자산       : {result['final_equity']:,.4f} USDT")
    print(f"  총 수익률       : {result['total_return_pct']:.4f}%")
    print(f"  Sharpe Ratio    : {result['sharpe_ratio']:.4f}")
    print(f"  Max Drawdown    : {result['max_drawdown_pct']:.4f}%")
    print(f"  총 거래 수      : {result['total_trades']}")
    print(f"  승률            : {result['win_rate_pct']:.2f}%")
    print(f"  펀딩비 수취 횟수: {result['funding_collected_count']}")

    # JSON 저장
    out_path = RESULTS_DIR / "bt_funding_arb.json"
    _save_json(result, out_path)

    # DB 저장
    await _save_to_db(pool, result, args.symbol, start, end)
    await pool.close()

    print("\n[DONE] 백테스트 완료")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="펀딩비 차익거래 전략 백테스트")
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
