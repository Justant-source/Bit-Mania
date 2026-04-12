"""bt_dca_variants.py — Test C: DCA 추세 필터 변형 4종 비교 백테스트.

변형:
  1. baseline      : 24봉마다 매수, ±5%/3% 청산
  2. ema50_filter  : 가격이 50봉 EMA 위일 때만 매수
  3. dual_filter   : 가격이 200봉 EMA 위일 때만 매수
  4. graduated     : EMA50/200 기반 단계별 자본 배분

기간: 2023-04-01 ~ 2026-03-31 (3년)
저장: strategy_variant_results 테이블 (test_name="test_c_dca_variants")
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
from tests.backtest.core.constants import TAKER_FEE

log = structlog.get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

INITIAL_CAPITAL = 10_000.0
FEE_RATE = TAKER_FEE
SYMBOL = "BTCUSDT"
TIMEFRAME = "1h"
START_DATE = "2023-04-01"
END_DATE = "2026-03-31"
WARMUP_BARS = 200   # 충분한 EMA200 워밍업

# 서브기간 정의 (비교표 출력용)
SUB_PERIODS = {
    "bull_2023_24": ("2023-04-01", "2024-12-31"),
    "bear_2025h2":  ("2025-10-01", "2026-03-31"),
}

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CREATE_VARIANT_RESULTS = """
CREATE TABLE IF NOT EXISTS strategy_variant_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    test_name       TEXT        NOT NULL,
    variant         TEXT        NOT NULL,
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


# ── 지표 계산 ──────────────────────────────────────────────────────────────────

def _compute_ema(closes: np.ndarray, period: int) -> float:
    """지수이동평균 — 마지막 값 반환."""
    if len(closes) == 0:
        return 0.0
    k = 2.0 / (period + 1)
    ema = float(closes[0])
    for c in closes[1:]:
        ema = float(c) * k + ema * (1.0 - k)
    return ema


# ── 신호 함수 ──────────────────────────────────────────────────────────────────
# 반환값: None | "buy" | ("buy", capital_ratio)

def signal_baseline(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """baseline: 24봉마다 매수, TP+5%/SL-3%."""
    if idx % 24 == 0 and position is None:
        return "buy"
    if position is not None:
        entry = position["entry_price"]
        price = float(bar["close"])
        pct = (price - entry) / entry
        if pct >= 0.05 or pct <= -0.03:
            return "close"
    return None


def signal_ema50_filter(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """ema50_filter: 가격이 EMA50 위일 때만 매수."""
    closes = lookback["close"].values.astype(float)
    src = closes[-50:] if len(closes) >= 50 else closes
    ema50 = _compute_ema(src, 50)

    price = float(bar["close"])

    if idx % 24 == 0 and position is None:
        if price > ema50:
            return "buy"
        # EMA50 아래 → 진입 안 함
    if position is not None:
        entry = position["entry_price"]
        pct = (price - entry) / entry
        if pct >= 0.05 or pct <= -0.03:
            return "close"
    return None


def signal_dual_filter(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """dual_filter: 가격이 EMA200 위일 때만 매수 (더 강한 필터)."""
    closes = lookback["close"].values.astype(float)
    src = closes[-200:] if len(closes) >= 200 else closes
    ema200 = _compute_ema(src, 200)

    price = float(bar["close"])

    if idx % 24 == 0 and position is None:
        if price > ema200:
            return "buy"
    if position is not None:
        entry = position["entry_price"]
        pct = (price - entry) / entry
        if pct >= 0.05 or pct <= -0.03:
            return "close"
    return None


def signal_graduated(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """graduated: EMA50/200 기준 단계별 자본 배분.

    EMA50 위: 1.0배 (정상 매수)
    EMA50 아래, EMA200 위: 0.5배 (절반 매수)
    EMA200 아래: 0.0배 (중단)
    """
    closes = lookback["close"].values.astype(float)
    src50  = closes[-50:]  if len(closes) >= 50  else closes
    src200 = closes[-200:] if len(closes) >= 200 else closes
    ema50  = _compute_ema(src50,  50)
    ema200 = _compute_ema(src200, 200)

    price = float(bar["close"])

    if idx % 24 == 0 and position is None:
        if price > ema50:
            capital_ratio = 1.0
        elif price > ema200:
            capital_ratio = 0.5
        else:
            capital_ratio = 0.0

        if capital_ratio > 0.0:
            return ("buy", capital_ratio)

    if position is not None:
        entry = position["entry_price"]
        pct = (price - entry) / entry
        if pct >= 0.05 or pct <= -0.03:
            return "close"
    return None


# ── 간단한 백테스트 엔진 ───────────────────────────────────────────────────────

class SimpleBacktester:
    """이벤트 루프 기반 단순 백테스터."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        initial_capital: float = INITIAL_CAPITAL,
        fee_rate: float = FEE_RATE,
    ) -> None:
        self.df = ohlcv.reset_index()
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.fee_rate = fee_rate
        self.position: dict | None = None
        self.trades: list[dict] = []
        self.equity_curve: list[float] = [initial_capital]

    def run(self, signal_fn) -> dict:
        bars = self.df
        for idx in range(WARMUP_BARS, len(bars)):
            bar = bars.iloc[idx]
            lookback = bars.iloc[max(0, idx - 200): idx + 1]
            signal = signal_fn(bar, lookback, idx, self.position)
            self._process_signal(signal, bar, idx)
            self.equity_curve.append(self.capital + self._unrealized_pnl(bar))

        # 강제 청산
        if self.position is not None:
            self._close(bars.iloc[-1])
        if self.equity_curve:
            self.equity_curve[-1] = self.capital

        return self._build_result()

    def _process_signal(self, signal: Any, bar: Any, idx: int) -> None:
        if signal is None:
            return

        # ("buy", ratio) 형태 처리
        if isinstance(signal, tuple):
            action, ratio = signal
        else:
            action = signal
            ratio = 1.0

        if action == "buy" and self.position is None:
            self._open(bar, "buy", ratio)
        elif action == "sell" and self.position is None:
            self._open(bar, "sell", ratio)
        elif action == "close" and self.position is not None:
            self._close(bar)

    def _open(self, bar: Any, side: str, capital_ratio: float = 1.0) -> None:
        price = float(bar["close"])
        alloc = self.capital * 0.95 * capital_ratio
        if alloc <= 0:
            return
        size = alloc / price
        fee = price * size * self.fee_rate
        self.capital -= fee
        ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        self.position = {
            "side": side,
            "entry_price": price,
            "size": size,
            "entry_ts": ts,
            "fee_paid": fee,
        }

    def _close(self, bar: Any) -> None:
        if self.position is None:
            return
        price = float(bar["close"])
        entry = self.position["entry_price"]
        size  = self.position["size"]
        side  = self.position["side"]
        fee_exit = price * size * self.fee_rate

        if side == "buy":
            pnl = (price - entry) * size - fee_exit
        else:
            pnl = (entry - price) * size - fee_exit

        self.capital += pnl
        close_ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        self.trades.append({
            "entry_price": entry,
            "exit_price": price,
            "side": side,
            "size": size,
            "pnl": pnl,
            "fee": self.position["fee_paid"] + fee_exit,
            "entry_ts": str(self.position.get("entry_ts", "")),
            "close_ts": str(close_ts),
        })
        self.position = None

    def _unrealized_pnl(self, bar: Any) -> float:
        if self.position is None:
            return 0.0
        price = float(bar["close"])
        entry = self.position["entry_price"]
        size  = self.position["size"]
        if self.position["side"] == "buy":
            return (price - entry) * size
        return (entry - price) * size

    def _build_result(self) -> dict:
        total_profit = self.capital - self.initial_capital
        total_profit_pct = (total_profit / self.initial_capital * 100) if self.initial_capital > 0 else 0.0

        winning = [t for t in self.trades if t["pnl"] > 0]
        losing  = [t for t in self.trades if t["pnl"] <= 0]
        gross_profit = sum(t["pnl"] for t in winning)
        gross_loss   = abs(sum(t["pnl"] for t in losing))

        win_rate = (len(winning) / len(self.trades) * 100) if self.trades else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

        # MDD
        max_dd_pct = _compute_mdd_pct(self.equity_curve)

        # Sharpe
        daily_returns = _compute_daily_returns(self.equity_curve)
        sharpe = _compute_sharpe(daily_returns)

        return {
            "initial_capital": self.initial_capital,
            "final_equity": round(self.capital, 4),
            "total_profit": round(total_profit, 4),
            "total_profit_pct": round(total_profit_pct, 4),
            "max_drawdown_pct": round(max_dd_pct, 4),
            "sharpe_ratio": round(sharpe, 4),
            "win_rate": round(win_rate, 2),
            "total_trades": len(self.trades),
            "profit_factor": _safe_float(profit_factor),
            "equity_curve": self.equity_curve,
            "daily_returns": daily_returns,
            "trades": self.trades,
        }


# ── 서브기간 수익률 계산 ──────────────────────────────────────────────────────

def _subperiod_return(
    equity_curve: list[float],
    df_bars: pd.DataFrame,
    start_str: str,
    end_str: str,
) -> float:
    """서브기간 시작/끝 equity 인덱스로 수익률 계산."""
    start_dt = pd.Timestamp(start_str, tz="UTC")
    end_dt   = pd.Timestamp(end_str,   tz="UTC")

    # bars는 reset_index 되어 있고 'ts' 컬럼이 있어야 함
    ts_col = df_bars["ts"] if "ts" in df_bars.columns else df_bars.index.to_series()
    ts_col = pd.to_datetime(ts_col, utc=True)

    mask_start = ts_col >= start_dt
    mask_end   = ts_col <= end_dt

    idx_start = mask_start.idxmax() if mask_start.any() else 0
    idx_end   = (ts_col[mask_end].index[-1]) if mask_end.any() else len(equity_curve) - 1

    # equity_curve의 인덱스와 bars 인덱스 오프셋(WARMUP_BARS)을 감안
    eq_offset = WARMUP_BARS  # equity_curve[0] 은 warmup 이전 initial capital
    eq_start_idx = max(0, int(idx_start) - eq_offset + 1)
    eq_end_idx   = min(len(equity_curve) - 1, int(idx_end) - eq_offset + 1)

    if eq_start_idx >= len(equity_curve) or eq_end_idx < eq_start_idx:
        return 0.0

    eq_start = equity_curve[eq_start_idx]
    eq_end   = equity_curve[eq_end_idx]
    if eq_start <= 0:
        return 0.0
    return (eq_end - eq_start) / eq_start * 100.0


# ── 통계 유틸 ─────────────────────────────────────────────────────────────────

def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or math.isnan(v) or math.isinf(v):
        return default
    return float(v)


def _compute_mdd_pct(equity_curve: list[float]) -> float:
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd * 100.0


def _compute_daily_returns(equity_curve: list[float]) -> list[float]:
    if len(equity_curve) < 2:
        return []
    return [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        if equity_curve[i - 1] != 0 else 0.0
        for i in range(1, len(equity_curve))
    ]


def _compute_sharpe(returns: list[float], periods: int = 8760) -> float:
    """시간봉 기준 연환산 Sharpe."""
    if len(returns) < 2:
        return 0.0
    mean_r = sum(returns) / len(returns)
    var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0:
        return 0.0
    return (mean_r / std) * math.sqrt(periods)


# ── DB 연결 ────────────────────────────────────────────────────────────────────

async def _load_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM ohlcv_history
            WHERE symbol = $1 AND timeframe = $2
              AND timestamp >= $3 AND timestamp <= $4
            ORDER BY timestamp ASC
            """,
            symbol, timeframe, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


async def _save_variant(
    pool: asyncpg.Pool,
    test_name: str,
    variant: str,
    result: dict,
    symbol: str,
    start: datetime,
    end: datetime,
    extra_meta: dict | None = None,
) -> None:
    metadata = {
        "profit_factor": _safe_float(result["profit_factor"]),
        "win_rate": result["win_rate"],
        "total_trades": result["total_trades"],
        "equity_curve_sample": [
            round(_safe_float(v), 2)
            for v in result["equity_curve"][::max(1, len(result["equity_curve"]) // 200)]
        ],
        **(extra_meta or {}),
    }

    async with pool.acquire() as conn:
        await conn.execute(CREATE_VARIANT_RESULTS)
        await conn.execute(
            """
            INSERT INTO strategy_variant_results
                (test_name, variant_name, symbol, start_date, end_date,
                 initial_capital, final_equity, total_return,
                 sharpe_ratio, max_drawdown, win_rate, total_trades, metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
            """,
            test_name, variant, symbol, start, end,
            result["initial_capital"],
            result["final_equity"],
            result["total_profit_pct"],
            result["sharpe_ratio"],
            result["max_drawdown_pct"],
            result["win_rate"],
            result["total_trades"],
            json.dumps(metadata),
        )


# ── 메인 ──────────────────────────────────────────────────────────────────────

VARIANTS = {
    "baseline":     signal_baseline,
    "ema50_filter": signal_ema50_filter,
    "dual_filter":  signal_dual_filter,
    "graduated":    signal_graduated,
}


async def main(args: argparse.Namespace) -> None:
    import logging
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            kst_timestamper,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    log.info("connecting_db", dsn=DB_DSN.split("@")[1])
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)

    log.info("loading_ohlcv", symbol=args.symbol, start=args.start, end=args.end)
    ohlcv = await _load_ohlcv(pool, args.symbol, TIMEFRAME, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    log.info("ohlcv_loaded", rows=len(ohlcv))

    # 기존 test_c 데이터 삭제
    async with pool.acquire() as conn:
        await conn.execute(CREATE_VARIANT_RESULTS)
        deleted = await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = 'test_c_dca_variants'"
        )
        log.info("cleared_previous", deleted=deleted)

    results: dict[str, dict] = {}

    for variant_name, signal_fn in VARIANTS.items():
        log.info("running_variant", variant=variant_name)

        bt = SimpleBacktester(ohlcv, initial_capital=args.capital, fee_rate=FEE_RATE)
        result = bt.run(signal_fn)
        results[variant_name] = result

        # 서브기간 수익률
        df_bars = ohlcv.reset_index()
        sub_returns: dict[str, float] = {}
        for sub_label, (sub_start, sub_end) in SUB_PERIODS.items():
            ret = _subperiod_return(result["equity_curve"], df_bars, sub_start, sub_end)
            sub_returns[sub_label] = round(ret, 4)

        await _save_variant(
            pool,
            test_name="test_c_dca_variants",
            variant=variant_name,
            result=result,
            symbol=args.symbol,
            start=start_dt,
            end=end_dt,
            extra_meta={"sub_period_returns": sub_returns},
        )
        log.info(
            "variant_done",
            variant=variant_name,
            return_pct=round(result["total_profit_pct"], 2),
            sharpe=round(result["sharpe_ratio"], 4),
            mdd=round(result["max_drawdown_pct"], 2),
            trades=result["total_trades"],
        )

    # ── 비교표 출력 ────────────────────────────────────────────────────────────
    df_bars = ohlcv.reset_index()

    print("\n" + "=" * 100)
    print(f"{'변형':<16} | {'3년수익률':>9} | {'상승장(2023-24)':>14} | {'하락장(2025H2)':>14} | {'Sharpe':>7} | {'MDD':>7} | {'거래수':>5}")
    print("=" * 100)
    for variant_name, result in results.items():
        ret_3y   = result["total_profit_pct"]
        sharpe   = result["sharpe_ratio"]
        mdd      = result["max_drawdown_pct"]
        trades   = result["total_trades"]

        bull_ret = _subperiod_return(
            result["equity_curve"], df_bars,
            SUB_PERIODS["bull_2023_24"][0], SUB_PERIODS["bull_2023_24"][1],
        )
        bear_ret = _subperiod_return(
            result["equity_curve"], df_bars,
            SUB_PERIODS["bear_2025h2"][0], SUB_PERIODS["bear_2025h2"][1],
        )

        print(
            f"{variant_name:<16} | {ret_3y:>+8.2f}% | {bull_ret:>+13.2f}% | "
            f"{bear_ret:>+13.2f}% | {sharpe:>7.3f} | {mdd:>6.2f}% | {trades:>5}"
        )
    print("=" * 100)

    await pool.close()
    log.info("test_c_complete")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test C: DCA 추세 필터 변형 4종 비교 백테스트"
    )
    parser.add_argument("--symbol",  default=SYMBOL,      help="심볼 (기본: BTCUSDT)")
    parser.add_argument("--start",   default=START_DATE,  help="시작일 YYYY-MM-DD")
    parser.add_argument("--end",     default=END_DATE,    help="종료일 YYYY-MM-DD")
    parser.add_argument("--capital", default=INITIAL_CAPITAL, type=float,
                        help="초기 자본 USDT (기본: 10000)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
