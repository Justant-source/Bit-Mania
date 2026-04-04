"""bt_grid_replacement.py — Test D: 그리드 대체 후보 3종 비교 백테스트.

후보:
  1. mean_reversion : BB + RSI + ADX 기반 평균회귀 (ADX>25 추세장 차단)
  2. basis_spread   : 현물-선물 24봉 괴리 수렴 전략
  3. vol_selling    : 변동성 급등 후 SMA 복귀 베팅 (소량)

그리드 기준선도 함께 실행하여 4종 비교.
기간: 2023-04-01 ~ 2026-03-31 (3년)
저장: strategy_variant_results 테이블 (test_name="test_d_grid_replacement")
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

log = structlog.get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

INITIAL_CAPITAL  = 10_000.0
FEE_RATE         = 0.00055
SYMBOL           = "BTCUSDT"
TIMEFRAME        = "1h"
START_DATE       = "2023-04-01"
END_DATE         = "2026-03-31"
WARMUP_BARS      = 200   # EMA200, ADX 워밍업

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


# ── 지표 유틸 ─────────────────────────────────────────────────────────────────

def _ema_series(values: np.ndarray, period: int) -> np.ndarray:
    """EMA 시리즈 반환 (전체 길이)."""
    k = 2.0 / (period + 1)
    out = np.empty(len(values), dtype=float)
    out[0] = values[0]
    for i in range(1, len(values)):
        out[i] = float(values[i]) * k + out[i - 1] * (1.0 - k)
    return out


def _compute_rsi(closes: np.ndarray, period: int = 14) -> float:
    """RSI(period) — 마지막 값."""
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes.astype(float))
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """ATR(period) — 마지막 값."""
    if len(closes) < 2:
        return 0.0
    tr_list: list[float] = []
    for i in range(1, len(closes)):
        hl = float(highs[i]) - float(lows[i])
        hc = abs(float(highs[i]) - float(closes[i - 1]))
        lc = abs(float(lows[i]) - float(closes[i - 1]))
        tr_list.append(max(hl, hc, lc))
    tr_arr = np.array(tr_list, dtype=float)
    if len(tr_arr) == 0:
        return 0.0
    # 단순 평균 ATR (마지막 period 봉)
    return float(np.mean(tr_arr[-period:]))


def _compute_adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """ADX(period) — 마지막 값."""
    n = len(closes)
    if n < period * 2:
        return 0.0

    plus_dm_list: list[float] = []
    minus_dm_list: list[float] = []
    tr_list: list[float] = []

    for i in range(1, n):
        up   = float(highs[i])   - float(highs[i - 1])
        down = float(lows[i - 1]) - float(lows[i])
        plus_dm_list.append(up   if up > down and up > 0 else 0.0)
        minus_dm_list.append(down if down > up and down > 0 else 0.0)
        hl = float(highs[i]) - float(lows[i])
        hc = abs(float(highs[i]) - float(closes[i - 1]))
        lc = abs(float(lows[i]) - float(closes[i - 1]))
        tr_list.append(max(hl, hc, lc))

    if len(tr_list) < period:
        return 0.0

    atr_val = float(np.mean(tr_list[-period:]))
    if atr_val == 0:
        return 0.0

    plus_di  = 100.0 * float(np.mean(plus_dm_list[-period:]))  / atr_val
    minus_di = 100.0 * float(np.mean(minus_dm_list[-period:])) / atr_val
    di_sum = plus_di + minus_di
    if di_sum == 0:
        return 0.0
    return 100.0 * abs(plus_di - minus_di) / di_sum


def _compute_bb(closes: np.ndarray, period: int = 20, std_mult: float = 2.0):
    """볼린저 밴드 — (upper, mid, lower) 마지막 값."""
    src = closes[-period:] if len(closes) >= period else closes
    mid = float(np.mean(src))
    std = float(np.std(src))
    return mid + std_mult * std, mid, mid - std_mult * std


def _compute_sma(closes: np.ndarray, period: int) -> float:
    src = closes[-period:] if len(closes) >= period else closes
    return float(np.mean(src))


def _compute_atr_mean(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    atr_period: int = 14, mean_period: int = 50,
) -> tuple[float, float]:
    """(current_atr, mean_atr_over_last_mean_period_bars)."""
    n = len(closes)
    if n < atr_period + mean_period + 1:
        atr_now = _compute_atr(highs, lows, closes, atr_period)
        return atr_now, atr_now

    # 각 봉의 ATR 시리즈 구성 (마지막 mean_period개)
    atr_values: list[float] = []
    for offset in range(mean_period + 1):
        end_i = n - mean_period + offset
        if end_i < atr_period + 1:
            atr_values.append(0.0)
            continue
        h_slice = highs[:end_i]
        l_slice = lows[:end_i]
        c_slice = closes[:end_i]
        atr_values.append(_compute_atr(h_slice, l_slice, c_slice, atr_period))

    current_atr = atr_values[-1]
    mean_atr    = float(np.mean(atr_values[:-1])) if atr_values[:-1] else current_atr
    return current_atr, mean_atr


# ── 신호 함수 ──────────────────────────────────────────────────────────────────
# 반환: None | "buy" | "sell" | "close" | ("buy"/"sell", capital_ratio)

def _signal_mean_reversion(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """볼린저 밴드 + RSI + ADX 기반 평균회귀.

    롱 진입: close <= bb_lower AND RSI(14) < 30 AND ADX(14) < 25
    숏 진입: close >= bb_upper AND RSI(14) > 70 AND ADX(14) < 25
    청산: SMA20 복귀 OR 손실 > -2%
    ADX > 25: 추세장 → 아무것도 안 함
    """
    closes = lookback["close"].values.astype(float)
    highs  = lookback["high"].values.astype(float)  if "high"  in lookback.columns else closes
    lows   = lookback["low"].values.astype(float)   if "low"   in lookback.columns else closes

    price = float(bar["close"])

    bb_upper, sma20, bb_lower = _compute_bb(closes, period=20, std_mult=2.0)
    rsi  = _compute_rsi(closes, period=14)
    adx  = _compute_adx(highs, lows, closes, period=14)

    trending = adx >= 25.0

    if position is None:
        if not trending:
            if price <= bb_lower and rsi < 30.0:
                return "buy"
            if price >= bb_upper and rsi > 70.0:
                return "sell"
    else:
        side  = position["side"]
        entry = position["entry_price"]
        pct   = (price - entry) / entry if side == "buy" else (entry - price) / entry

        # SMA20 복귀 또는 손실 -2% 초과
        if side == "buy"  and price >= sma20:
            return "close"
        if side == "sell" and price <= sma20:
            return "close"
        if pct < -0.02:
            return "close"

    return None


def _signal_basis_spread(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """현물-선물 24봉 괴리 수렴 전략.

    basis = (close_t - close_{t-24}) / close_{t-24}
    진입: |basis| > 0.02 → 괴리 반대 방향 포지션
    청산: |basis| < 0.005 OR 72봉 보유 초과
    """
    closes = lookback["close"].values.astype(float)
    price  = float(bar["close"])

    if len(closes) < 25:
        return None

    close_24h_ago = float(closes[-25])  # 현재 포함 25개 중 가장 오래된 것 = 24봉 전
    if close_24h_ago == 0:
        return None
    basis = (price - close_24h_ago) / close_24h_ago

    if position is None:
        if abs(basis) > 0.02:
            # 괴리 방향 반대 포지션: 가격이 위로 많이 올랐으면 숏 (수렴 기대)
            return "sell" if basis > 0 else "buy"
    else:
        entry_idx = position.get("entry_idx", idx)
        hold_bars = idx - entry_idx

        # 수렴 조건: basis가 0.5% 이내
        if abs(basis) < 0.005:
            return "close"
        # 72봉 보유 초과 → 강제 청산
        if hold_bars >= 72:
            return "close"

    return None


def _signal_vol_selling(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """변동성 급등 후 SMA 복귀 베팅 (소량 포지션, 자본 0.5배).

    진입: ATR(14) > ATR_mean(50봉) × 1.5
          AND close가 SMA20 ± 3% 이내
    청산: SMA20 복귀 OR 보유 24봉 OR 손실 > -1.5%
    포지션: 기본의 0.5배 (capital_ratio=0.5)
    """
    closes = lookback["close"].values.astype(float)
    highs  = lookback["high"].values.astype(float) if "high" in lookback.columns else closes
    lows   = lookback["low"].values.astype(float)  if "low"  in lookback.columns else closes
    price  = float(bar["close"])

    sma20 = _compute_sma(closes, 20)
    if sma20 == 0:
        return None
    near_sma = abs(price - sma20) / sma20 < 0.03

    atr_now, atr_mean = _compute_atr_mean(highs, lows, closes, atr_period=14, mean_period=50)
    high_vol = (atr_mean > 0) and (atr_now > atr_mean * 1.5)

    if position is None:
        if high_vol and near_sma:
            # 방향: 가격이 SMA 위면 숏 (SMA로 눌릴 거라는 베팅), 아래면 롱
            side = "sell" if price >= sma20 else "buy"
            return (side, 0.5)
    else:
        side       = position["side"]
        entry      = position["entry_price"]
        entry_idx  = position.get("entry_idx", idx)
        hold_bars  = idx - entry_idx
        pct        = (price - entry) / entry if side == "buy" else (entry - price) / entry

        if side == "buy"  and price >= sma20:
            return "close"
        if side == "sell" and price <= sma20:
            return "close"
        if hold_bars >= 24:
            return "close"
        if pct < -0.015:
            return "close"

    return None


def _signal_grid_baseline(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
) -> Any:
    """그리드 기준선: ADX < 25 + BB squeeze 시 진입.
    freqtrade_bridge._signal_grid와 동일 로직.
    """
    closes = lookback["close"].values.astype(float)
    highs  = lookback["high"].values.astype(float) if "high"  in lookback.columns else closes
    lows   = lookback["low"].values.astype(float)  if "low"   in lookback.columns else closes

    adx = _compute_adx(highs, lows, closes, period=14)

    bb_narrow = False
    bb_window = closes[-20:] if len(closes) >= 20 else closes
    if len(bb_window) >= 5:
        sma_bb = float(np.mean(bb_window))
        std_bb = float(np.std(bb_window))
        bb_width = (4.0 * std_bb / sma_bb) if sma_bb > 0 else 1.0
        bb_narrow = bb_width < 0.04

    ranging = adx < 25.0 and bb_narrow

    if position is None:
        if ranging:
            return "buy"
    else:
        if not ranging:
            return "close"
        entry = position["entry_price"]
        price = float(bar["close"])
        pct = (price - entry) / entry
        if pct < -0.01:
            return "close"
    return None


# ── 백테스트 엔진 ─────────────────────────────────────────────────────────────

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

        if isinstance(signal, tuple):
            action, ratio = signal
        else:
            action, ratio = signal, 1.0

        if action in ("buy", "sell") and self.position is None:
            self._open(bar, action, ratio, idx)
        elif action == "close" and self.position is not None:
            self._close(bar)

    def _open(self, bar: Any, side: str, capital_ratio: float = 1.0, idx: int = 0) -> None:
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
            "entry_idx": idx,
            "fee_paid": fee,
        }

    def _close(self, bar: Any) -> None:
        if self.position is None:
            return
        price    = float(bar["close"])
        entry    = self.position["entry_price"]
        size     = self.position["size"]
        side     = self.position["side"]
        fee_exit = price * size * self.fee_rate

        pnl = ((price - entry) if side == "buy" else (entry - price)) * size - fee_exit
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
        return ((price - entry) if self.position["side"] == "buy" else (entry - price)) * size

    def _build_result(self) -> dict:
        total_profit = self.capital - self.initial_capital
        total_profit_pct = (total_profit / self.initial_capital * 100) if self.initial_capital > 0 else 0.0

        winning = [t for t in self.trades if t["pnl"] > 0]
        losing  = [t for t in self.trades if t["pnl"] <= 0]
        gross_profit = sum(t["pnl"] for t in winning)
        gross_loss   = abs(sum(t["pnl"] for t in losing))

        win_rate      = (len(winning) / len(self.trades) * 100) if self.trades else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

        max_dd_pct   = _compute_mdd_pct(self.equity_curve)
        daily_returns = _compute_daily_returns(self.equity_curve)
        sharpe       = _compute_sharpe(daily_returns)

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


def _compute_trend_loss(result: dict) -> str:
    """추세장(ADX>25 구간) 대략적 손실 정도 표현."""
    # 거래 중 손실 거래 비율과 평균 손실로 가늠
    trades = result.get("trades", [])
    if not trades:
        return "N/A"
    losing = [t for t in trades if t["pnl"] < 0]
    if not losing:
        return "없음"
    avg_loss = abs(sum(t["pnl"] for t in losing) / len(losing))
    ratio = len(losing) / len(trades)
    if ratio > 0.6 or avg_loss > 100:
        return "큼"
    elif ratio > 0.4 or avg_loss > 30:
        return "중간"
    else:
        return "작음"


# ── DB 함수 ───────────────────────────────────────────────────────────────────

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
    eq = result["equity_curve"]
    eq_sample = [
        round(_safe_float(v), 2)
        for v in eq[::max(1, len(eq) // 200)]
    ]
    metadata = {
        "profit_factor": _safe_float(result["profit_factor"]),
        "win_rate": result["win_rate"],
        "total_trades": result["total_trades"],
        "equity_curve_sample": eq_sample,
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
    "grid (기존)":   _signal_grid_baseline,
    "mean_reversion": _signal_mean_reversion,
    "basis_spread":   _signal_basis_spread,
    "vol_selling":    _signal_vol_selling,
}

# DB 저장용 안전한 키 이름
VARIANT_DB_NAMES = {
    "grid (기존)":    "grid_baseline",
    "mean_reversion": "mean_reversion",
    "basis_spread":   "basis_spread",
    "vol_selling":    "vol_selling",
}


async def main(args: argparse.Namespace) -> None:
    import logging
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
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

    # 기존 test_d 데이터 삭제
    async with pool.acquire() as conn:
        await conn.execute(CREATE_VARIANT_RESULTS)
        deleted = await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = 'test_d_grid_replacement'"
        )
        log.info("cleared_previous", deleted=deleted)

    results: dict[str, dict] = {}

    for display_name, signal_fn in VARIANTS.items():
        db_name = VARIANT_DB_NAMES[display_name]
        log.info("running_variant", variant=display_name)

        bt = SimpleBacktester(ohlcv, initial_capital=args.capital, fee_rate=FEE_RATE)
        result = bt.run(signal_fn)
        results[display_name] = result

        await _save_variant(
            pool,
            test_name="test_d_grid_replacement",
            variant=db_name,
            result=result,
            symbol=args.symbol,
            start=start_dt,
            end=end_dt,
        )
        log.info(
            "variant_done",
            variant=display_name,
            return_pct=round(result["total_profit_pct"], 2),
            sharpe=round(result["sharpe_ratio"], 4),
            mdd=round(result["max_drawdown_pct"], 2),
            trades=result["total_trades"],
        )

    # ── 비교표 출력 ────────────────────────────────────────────────────────────
    print("\n" + "=" * 95)
    print(
        f"{'전략':<16} | {'3년수익률':>9} | {'Sharpe':>7} | {'MDD':>8} | "
        f"{'거래수':>6} | {'추세장손실':>10}"
    )
    print("=" * 95)
    for display_name, result in results.items():
        ret    = result["total_profit_pct"]
        sharpe = result["sharpe_ratio"]
        mdd    = result["max_drawdown_pct"]
        trades = result["total_trades"]
        trend_loss = _compute_trend_loss(result)

        print(
            f"{display_name:<16} | {ret:>+8.2f}% | {sharpe:>7.3f} | "
            f"{mdd:>7.2f}% | {trades:>6} | {trend_loss:>10}"
        )
    print("=" * 95)

    await pool.close()
    log.info("test_d_complete")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test D: 그리드 대체 후보 3종 + 그리드 기준선 비교 백테스트"
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
