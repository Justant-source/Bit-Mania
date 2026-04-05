"""Test B — 펀딩비 전략 4가지 변형 비교 (3년 데이터)

strategy_variant_results 테이블에 결과 저장 후 콘솔 비교표 출력.

변형:
  baseline       — 즉시 역전 청산, 연속 3회 양수 진입
  tolerant_exit  — 24시간(3회 연속) 역전 시 청산
  strict_entry   — 연속 5회 양수 + min_rate 0.0002
  short_hold     — max_hold_bars=168 (7일) 강제 청산
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pandas as pd
import structlog

from freqtrade_bridge import (
    BacktestResult,
    TradeRecord,
    _compute_daily_returns,
    _compute_drawdown,
    _compute_sharpe,
    _compute_sortino,
    _drawdown_series,
)

log = structlog.get_logger(__name__)

# ── DB 연결 ──────────────────────────────────────────────────────────────────
DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
TEST_NAME       = "test_f_funding_arb_variants_6y"

# ── 4가지 변형 파라미터 ───────────────────────────────────────────────────────
VARIANTS: dict[str, dict[str, Any]] = {
    "baseline": {
        "exit_on_flip": True,
        "negative_hours_before_exit": 0,
        "consecutive_intervals": 3,
        "min_funding_rate": 0.0001,
        "max_hold_bars": 90,
    },
    "tolerant_exit": {
        "exit_on_flip": False,
        "negative_hours_before_exit": 24,   # 3회 연속 8h 역전 = 24h
        "consecutive_intervals": 3,
        "min_funding_rate": 0.0001,
        "max_hold_bars": 90,
    },
    "strict_entry": {
        "exit_on_flip": True,
        "negative_hours_before_exit": 0,
        "consecutive_intervals": 5,
        "min_funding_rate": 0.0002,
        "max_hold_bars": 90,
    },
    "short_hold": {
        "exit_on_flip": True,
        "negative_hours_before_exit": 0,
        "consecutive_intervals": 3,
        "min_funding_rate": 0.0001,
        "max_hold_bars": 168,
    },
}


# =========================================================================
# 파라미터화된 펀딩비 백테스트 엔진
# =========================================================================

class _FundingArbVariantEngine:
    """VARIANTS 파라미터를 받아 펀딩비 전략을 실행하는 독립 이벤트 루프."""

    def __init__(
        self,
        *,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        params: dict[str, Any],
        initial_capital: float = INITIAL_CAPITAL,
        fee_rate: float = FEE_RATE,
    ) -> None:
        self._ohlcv    = ohlcv
        self._funding  = funding
        self._params   = params
        self._capital  = initial_capital
        self._fee_rate = fee_rate

        self._equity        = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[TradeRecord] = []
        self._position: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # 메인 루프
    # ------------------------------------------------------------------

    def run(self) -> BacktestResult:
        exit_on_flip      = self._params["exit_on_flip"]
        neg_thresh        = self._params["negative_hours_before_exit"]  # 시간
        consec_intervals  = self._params["consecutive_intervals"]
        min_rate          = self._params["min_funding_rate"]
        max_hold          = self._params["max_hold_bars"]

        # 8h 역전 횟수 기준으로 변환 (1회 = 8시간)
        neg_consec_thresh = max(1, neg_thresh // 8) if neg_thresh > 0 else 0

        bars = self._ohlcv.reset_index()
        n    = len(bars)

        # 진입 전 연속 조건 카운터
        pos_consec_count  = 0   # 양수 펀딩 연속 횟수
        neg_consec_count  = 0   # 음수 펀딩 연속 횟수 (tolerant_exit용)

        for idx in range(20, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding_rate(bar)

            # ── 8h 정산 타이밍 감지 ─────────────────────────────────────
            ts = bar.get("ts", bar.name)
            try:
                ts_dt = pd.Timestamp(ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.tz_localize("UTC")
                is_settlement = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
            except Exception:
                is_settlement = (idx % 8 == 0)

            # ── 펀딩비 정산: equity에 즉시 반영 ──────────────────────────
            if self._position is not None and is_settlement:
                direction = self._position.get("funding_direction", 1)
                pos_value = self._position["size"] * self._position["entry_price"]
                net_funding = pos_value * funding * direction
                self._equity += net_funding
                self._position["funding_accumulated"] = (
                    self._position.get("funding_accumulated", 0.0) + net_funding
                )

            # ── 진입 로직 ────────────────────────────────────────────────
            if self._position is None:
                if is_settlement:
                    if funding >= min_rate:
                        pos_consec_count = pos_consec_count + 1
                        neg_consec_count = 0
                    elif funding <= -min_rate:
                        neg_consec_count = neg_consec_count + 1
                        pos_consec_count = 0
                    else:
                        pos_consec_count = 0
                        neg_consec_count = 0

                    # 연속 N회 조건 충족 시 진입
                    if pos_consec_count >= consec_intervals:
                        self._open_position(bar, "sell", idx)  # 숏 perp
                        pos_consec_count = 0
                    elif neg_consec_count >= consec_intervals:
                        self._open_position(bar, "buy", idx)   # 롱 perp
                        neg_consec_count = 0

            # ── 청산 로직 ────────────────────────────────────────────────
            else:
                direction    = self._position.get("funding_direction", 1)
                bars_held    = idx - self._position.get("entry_idx", idx)
                reversed_now = (direction > 0 and funding < 0) or \
                               (direction < 0 and funding > 0)

                should_close = False

                if exit_on_flip:
                    # baseline / strict_entry / short_hold: 역전 즉시 카운트
                    if is_settlement:
                        if reversed_now:
                            self._position["reverse_count"] = (
                                self._position.get("reverse_count", 0) + 1
                            )
                        else:
                            self._position["reverse_count"] = 0
                        if self._position.get("reverse_count", 0) >= 3:
                            should_close = True
                else:
                    # tolerant_exit: neg_consec_thresh 연속 역전 시 청산
                    if is_settlement:
                        if reversed_now:
                            self._position["reverse_count"] = (
                                self._position.get("reverse_count", 0) + 1
                            )
                        else:
                            self._position["reverse_count"] = 0
                        if self._position.get("reverse_count", 0) >= neg_consec_thresh:
                            should_close = True

                # max_hold_bars 강제 청산
                if bars_held >= max_hold:
                    should_close = True

                if should_close:
                    self._close_position(bar)
                    pos_consec_count = 0
                    neg_consec_count = 0

            self._equity_curve.append(
                self._equity + self._unrealized_pnl()
            )

        # 마지막 포지션 강제 청산
        if self._position is not None:
            self._close_position(bars.iloc[-1])
            self._equity_curve[-1] = self._equity

        return self._build_result(bars)

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------

    def _get_funding_rate(self, bar: Any) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        if ts is None:
            return 0.0001
        try:
            ts = pd.Timestamp(ts)
            mask = self._funding.index <= ts
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    def _open_position(self, bar: Any, side: str, idx: int) -> None:
        entry = float(bar["close"])
        size  = (self._equity * 0.95) / entry
        fee   = entry * size * self._fee_rate
        self._equity -= fee
        self._position = {
            "side": side,
            "entry_price": entry,
            "size": size,
            "entry_ts": bar.get("ts", bar.name) if hasattr(bar, "name") else None,
            "entry_idx": idx,
            "fee_paid": fee,
            "funding_direction": 1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count": 0,
        }

    def _close_position(self, bar: Any) -> None:
        if self._position is None:
            return
        size      = self._position["size"]
        entry     = self._position["entry_price"]
        entry_ts  = self._position.get("entry_ts")
        close_ts  = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        fee_entry = self._position.get("fee_paid", 0.0)
        exit_price = float(bar["close"])
        fee_exit   = exit_price * size * self._fee_rate
        self._equity -= fee_exit

        net_pnl = self._position.get("funding_accumulated", 0.0) - fee_entry - fee_exit

        self._trades.append(
            TradeRecord(
                open_ts=pd.Timestamp(entry_ts) if entry_ts else datetime.min,
                close_ts=pd.Timestamp(close_ts) if close_ts else datetime.min,
                symbol=SYMBOL,
                side=self._position["side"],
                quantity=size,
                entry_price=entry,
                exit_price=exit_price,
                pnl=net_pnl,
                fee=fee_entry + fee_exit,
                duration_hours=0.0,
            )
        )
        self._position = None

    def _unrealized_pnl(self) -> float:
        # 델타 뉴트럴 — 가격 손익 상쇄
        return 0.0

    def _build_result(self, bars: pd.DataFrame) -> BacktestResult:
        total_profit = self._equity - self._capital
        winning      = [t for t in self._trades if t.pnl > 0]
        losing       = [t for t in self._trades if t.pnl <= 0]
        gross_profit = sum(t.pnl for t in winning)
        gross_loss   = abs(sum(t.pnl for t in losing))

        max_dd, max_dd_pct = _compute_drawdown(self._equity_curve)
        daily_returns      = _compute_daily_returns(self._equity_curve)
        sharpe  = _compute_sharpe(daily_returns)
        sortino = _compute_sortino(daily_returns)
        dd_curve = _drawdown_series(self._equity_curve)

        start_date = str(bars.iloc[0].get("ts", "")) if len(bars) > 0 else ""
        end_date   = str(bars.iloc[-1].get("ts", "")) if len(bars) > 0 else ""

        return BacktestResult(
            strategy="funding_arb",
            start_date=start_date,
            end_date=end_date,
            initial_capital=self._capital,
            final_capital=self._equity,
            total_profit=total_profit,
            total_profit_pct=(total_profit / self._capital * 100) if self._capital > 0 else 0.0,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            win_rate=(len(winning) / len(self._trades) * 100) if self._trades else 0.0,
            total_trades=len(self._trades),
            avg_trade_duration_hours=0.0,
            profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else float("inf"),
            trades=self._trades,
            equity_curve=self._equity_curve,
            drawdown_curve=dd_curve,
            daily_returns=daily_returns,
        )


# =========================================================================
# DB 헬퍼
# =========================================================================

async def load_ohlcv(
    pool: asyncpg.Pool, symbol: str, timeframe: str,
    start: datetime, end: datetime,
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


async def load_funding(
    pool: asyncpg.Pool, symbol: str,
    start: datetime, end: datetime,
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate
            FROM funding_rate_history
            WHERE symbol = $1
              AND timestamp >= $2 AND timestamp <= $3
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


def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or math.isnan(v) or math.isinf(v):
        return default
    return v


def _monthly_returns(daily_returns: list[float], start_str: str) -> dict[str, float]:
    if not daily_returns:
        return {}
    try:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return {}
    monthly: dict[str, float] = {}
    for i, ret in enumerate(daily_returns):
        day = pd.Timestamp(start_dt) + pd.Timedelta(hours=i)
        key = day.strftime("%Y-%m")
        monthly[key] = monthly.get(key, 0.0) + _safe_float(ret)
    return monthly


async def save_variant_result(
    pool: asyncpg.Pool,
    variant_name: str,
    result: BacktestResult,
    params: dict[str, Any],
) -> None:
    monthly = _monthly_returns(result.daily_returns, START_DATE)
    eq_curve = result.equity_curve
    if len(eq_curve) > 200:
        step = max(1, len(eq_curve) // 200)
        eq_curve = eq_curve[::step]

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO strategy_variant_results
                (test_name, variant_name, data_range,
                 total_return, sharpe_ratio, max_drawdown,
                 trade_count, win_rate, profit_factor,
                 monthly_returns, params)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
            ON CONFLICT DO NOTHING
            """,
            TEST_NAME,
            variant_name,
            f"{START_DATE}~{END_DATE}",
            _safe_float(result.total_profit_pct),
            _safe_float(result.sharpe_ratio),
            _safe_float(result.max_drawdown_pct),
            result.total_trades,
            _safe_float(result.win_rate),
            _safe_float(result.profit_factor, default=0.0),
            json.dumps(monthly),
            json.dumps({**params,
                        "equity_curve_sample": [round(_safe_float(v), 2) for v in eq_curve]}),
        )
    log.info(
        "variant_saved",
        variant=variant_name,
        return_pct=round(_safe_float(result.total_profit_pct), 2),
        sharpe=round(_safe_float(result.sharpe_ratio), 2),
        trades=result.total_trades,
    )


# =========================================================================
# 메인
# =========================================================================

async def main() -> None:
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

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(END_DATE,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    log.info("loading_data", symbol=SYMBOL, timeframe=TIMEFRAME,
             start=START_DATE, end=END_DATE)
    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        log.error("no_ohlcv_data", hint="먼저 fetch_real_ohlcv.py 또는 seed_historical.py를 실행하세요.")
        await pool.close()
        return

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # 기존 결과 삭제 (재실행 시 중복 방지)
    async with pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = $1",
            TEST_NAME,
        )
        log.info("cleared_previous", deleted=deleted)

    results: dict[str, BacktestResult] = {}

    for variant_name, params in VARIANTS.items():
        log.info("running_variant", variant=variant_name, params=params)
        engine = _FundingArbVariantEngine(
            ohlcv=ohlcv,
            funding=funding,
            params=params,
            initial_capital=INITIAL_CAPITAL,
            fee_rate=FEE_RATE,
        )
        result = engine.run()
        results[variant_name] = result
        await save_variant_result(pool, variant_name, result, params)

    # ── 콘솔 비교표 ──────────────────────────────────────────────────────
    print("\n" + "=" * 90)
    print(f"Test B — 펀딩비 전략 변형 비교 ({START_DATE} ~ {END_DATE})")
    print("=" * 90)
    header = f"{'변형':<16} {'수익률%':>8} {'Sharpe':>8} {'MDD%':>8} {'거래수':>6} {'승률%':>7} {'PF':>6}"
    print(header)
    print("-" * 90)

    for vname, r in results.items():
        pf_str = f"{_safe_float(r.profit_factor):.3f}" if not math.isinf(r.profit_factor) else "  inf"
        print(
            f"{vname:<16} "
            f"{_safe_float(r.total_profit_pct):>8.2f} "
            f"{_safe_float(r.sharpe_ratio):>8.3f} "
            f"{_safe_float(r.max_drawdown_pct):>8.2f} "
            f"{r.total_trades:>6d} "
            f"{_safe_float(r.win_rate):>7.1f} "
            f"{pf_str:>6}"
        )

    print("=" * 90)
    print()

    # 파라미터 요약
    print("[ 변형 파라미터 요약 ]")
    print(f"{'변형':<16} {'exit_on_flip':<14} {'neg_hrs':<9} {'consec':<8} {'min_rate':<10} {'max_bars'}")
    print("-" * 70)
    for vname, p in VARIANTS.items():
        print(
            f"{vname:<16} "
            f"{str(p['exit_on_flip']):<14} "
            f"{p['negative_hours_before_exit']:<9} "
            f"{p['consecutive_intervals']:<8} "
            f"{p['min_funding_rate']:<10.4f} "
            f"{p['max_hold_bars']}"
        )
    print("=" * 70)

    await pool.close()
    log.info("test_b_complete")


if __name__ == "__main__":
    asyncio.run(main())
