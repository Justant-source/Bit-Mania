"""bt_fa_profit_reinvest.py — Stage 0: FA 수익 재투자 시뮬레이션

FA short_hold 변형의 6년 백테스트를 직접 실행하고,
수익 발생 거래에서 수익의 N%를 BTC 현물로 재투자하는 시뮬레이션.

reinvest_ratio = [0.0, 0.2, 0.3, 0.5, 0.7, 1.0] 6가지 비율 비교.

기간: 2020-04-01 ~ 2026-03-31 (6년)
저장: strategy_variant_results 테이블 (test_name="test_11_stage0_reinvest")

실행 방법:
    # DB 연결 (기본: localhost:5432)
    python bt_fa_profit_reinvest.py

    # 커스텀 기간
    python bt_fa_profit_reinvest.py --start 2020-04-01 --end 2026-03-31

    # 환경변수로 DB 설정
    DB_HOST=postgres DB_PASSWORD=CryptoEngine2026! python bt_fa_profit_reinvest.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
log = structlog.get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
TEST_NAME       = "test_11_stage0_reinvest"

REINVEST_RATIOS = [0.0, 0.2, 0.3, 0.5, 0.7, 1.0]

# FA short_hold 파라미터
FA_SHORT_HOLD_PARAMS: dict[str, Any] = {
    "exit_on_flip": True,
    "negative_hours_before_exit": 0,
    "consecutive_intervals": 3,
    "min_funding_rate": 0.0001,
    "max_hold_bars": 168,
}

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_VARIANT_RESULTS = """
CREATE TABLE IF NOT EXISTS strategy_variant_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    test_name       TEXT        NOT NULL,
    variant_name    TEXT        NOT NULL,
    data_range      TEXT,
    total_return    DOUBLE PRECISION,
    sharpe_ratio    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION,
    trade_count     INTEGER,
    win_rate        DOUBLE PRECISION,
    profit_factor   DOUBLE PRECISION,
    monthly_returns JSONB,
    params          JSONB
);
"""


# ── DB 로드 ───────────────────────────────────────────────────────────────────

async def load_ohlcv(
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


async def load_funding(
    pool: asyncpg.Pool,
    symbol: str,
    start: datetime,
    end: datetime,
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
    df["rate"] = df["rate"].astype(float)
    return df


# ── FA short_hold 엔진 ────────────────────────────────────────────────────────

class FundingArbShortHoldEngine:
    """FA short_hold 변형 백테스트 엔진 (bt_funding_arb_variants.py 패턴 참조)."""

    def __init__(
        self,
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
        self._equity   = initial_capital
        self._fee_rate = fee_rate
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[dict] = []
        self._position: dict[str, Any] | None = None

    def run(self) -> dict:
        exit_on_flip     = self._params["exit_on_flip"]
        neg_thresh       = self._params["negative_hours_before_exit"]
        consec_intervals = self._params["consecutive_intervals"]
        min_rate         = self._params["min_funding_rate"]
        max_hold         = self._params["max_hold_bars"]

        neg_consec_thresh = max(1, neg_thresh // 8) if neg_thresh > 0 else 0

        bars = self._ohlcv.reset_index()
        n    = len(bars)

        pos_consec_count = 0
        neg_consec_count = 0

        for idx in range(20, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding_rate(bar)

            ts = bar["ts"] if "ts" in bar.index else bar.name
            try:
                ts_dt = pd.Timestamp(ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.tz_localize("UTC")
                is_settlement = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
            except Exception:
                is_settlement = (idx % 8 == 0)

            # 펀딩비 정산
            if self._position is not None and is_settlement:
                direction = self._position.get("funding_direction", 1)
                pos_value = self._position["size"] * self._position["entry_price"]
                net_funding = pos_value * funding * direction
                self._equity += net_funding
                self._position["funding_accumulated"] = (
                    self._position.get("funding_accumulated", 0.0) + net_funding
                )

            # 진입 로직
            if self._position is None:
                if is_settlement:
                    if funding >= min_rate:
                        pos_consec_count += 1
                        neg_consec_count  = 0
                    elif funding <= -min_rate:
                        neg_consec_count += 1
                        pos_consec_count  = 0
                    else:
                        pos_consec_count = 0
                        neg_consec_count = 0

                    if pos_consec_count >= consec_intervals:
                        self._open_position(bar, "sell", idx)
                        pos_consec_count = 0
                    elif neg_consec_count >= consec_intervals:
                        self._open_position(bar, "buy", idx)
                        neg_consec_count = 0
            else:
                direction    = self._position.get("funding_direction", 1)
                bars_held    = idx - self._position.get("entry_idx", idx)
                reversed_now = (direction > 0 and funding < 0) or \
                               (direction < 0 and funding > 0)

                should_close = False

                if exit_on_flip:
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
                    if is_settlement:
                        if reversed_now:
                            self._position["reverse_count"] = (
                                self._position.get("reverse_count", 0) + 1
                            )
                        else:
                            self._position["reverse_count"] = 0
                        if self._position.get("reverse_count", 0) >= neg_consec_thresh:
                            should_close = True

                if bars_held >= max_hold:
                    should_close = True

                if should_close:
                    self._close_position(bar)
                    pos_consec_count = 0
                    neg_consec_count = 0

            self._equity_curve.append(self._equity)

        # 마지막 포지션 강제 청산
        if self._position is not None:
            self._close_position(bars.iloc[-1])
            if self._equity_curve:
                self._equity_curve[-1] = self._equity

        return self._build_result()

    def _get_funding_rate(self, bar: Any) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        ts = bar["ts"] if "ts" in bar.index else bar.name
        if ts is None:
            return 0.0001
        try:
            ts_pd = pd.Timestamp(ts)
            mask = self._funding.index <= ts_pd
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
        ts = bar["ts"] if "ts" in bar.index else bar.name
        self._position = {
            "side": side,
            "entry_price": entry,
            "size": size,
            "entry_ts": ts,
            "entry_idx": idx,
            "fee_paid": fee,
            "funding_direction": 1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count": 0,
        }

    def _close_position(self, bar: Any) -> None:
        if self._position is None:
            return
        size       = self._position["size"]
        entry      = self._position["entry_price"]
        entry_ts   = self._position.get("entry_ts")
        close_ts   = bar["ts"] if "ts" in bar.index else bar.name
        fee_entry  = self._position.get("fee_paid", 0.0)
        exit_price = float(bar["close"])
        fee_exit   = exit_price * size * self._fee_rate
        self._equity -= fee_exit

        net_pnl = self._position.get("funding_accumulated", 0.0) - fee_entry - fee_exit

        self._trades.append({
            "entry_price": entry,
            "exit_price": exit_price,
            "side": self._position["side"],
            "size": size,
            "pnl": net_pnl,
            "fee": fee_entry + fee_exit,
            "funding_accumulated": self._position.get("funding_accumulated", 0.0),
            "entry_ts": str(entry_ts) if entry_ts else "",
            "close_ts": str(close_ts) if close_ts else "",
        })
        self._position = None

    def _build_result(self) -> dict:
        total_profit = self._equity - self._capital
        winning = [t for t in self._trades if t["pnl"] > 0]
        losing  = [t for t in self._trades if t["pnl"] <= 0]
        gross_profit = sum(t["pnl"] for t in winning)
        gross_loss   = abs(sum(t["pnl"] for t in losing))

        win_rate      = (len(winning) / len(self._trades) * 100) if self._trades else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
        total_pct     = (total_profit / self._capital * 100) if self._capital > 0 else 0.0

        return {
            "initial_capital": self._capital,
            "final_equity": round(self._equity, 4),
            "total_profit": round(total_profit, 4),
            "total_profit_pct": round(total_pct, 4),
            "win_rate": round(win_rate, 2),
            "total_trades": len(self._trades),
            "profit_factor": profit_factor,
            "equity_curve": self._equity_curve,
            "trades": self._trades,
        }


# ── 재투자 시뮬레이션 ─────────────────────────────────────────────────────────

def _calc_annual_return(total_pct: float, n_years: float = 6.0) -> float:
    """총 수익률 → 연수익률 변환 (CAGR)."""
    if n_years <= 0:
        return 0.0
    factor = 1.0 + total_pct / 100.0
    if factor <= 0:
        return -100.0
    return ((factor ** (1.0 / n_years)) - 1.0) * 100.0


def _calc_mdd_2022(
    ohlcv: pd.DataFrame,
    btc_qty_series: list[tuple[pd.Timestamp, float]],
) -> float:
    """2022년 구간 현물 포지션 MDD 계산.

    btc_qty_series: [(timestamp, cumulative_btc_qty), ...]
    """
    start_2022 = pd.Timestamp("2022-01-01", tz="UTC")
    end_2022   = pd.Timestamp("2022-12-31", tz="UTC")

    ohlcv_2022 = ohlcv[(ohlcv.index >= start_2022) & (ohlcv.index <= end_2022)]
    if ohlcv_2022.empty or not btc_qty_series:
        return 0.0

    # 2022년 시작 시점 누적 BTC 수량 찾기
    btc_at_2022_start = 0.0
    for ts, qty in btc_qty_series:
        if ts <= start_2022:
            btc_at_2022_start = qty
        else:
            break

    if btc_at_2022_start <= 0:
        # 2022년 중에 매수한 것들 포함
        pass

    # 2022년 구간 equity curve 구성 (현물 가치)
    spot_values: list[float] = []
    qty_idx = 0
    current_qty = btc_at_2022_start

    for ts, close in ohlcv_2022["close"].items():
        # 이 시점까지의 누적 BTC 수량 업데이트
        while qty_idx < len(btc_qty_series) and btc_qty_series[qty_idx][0] <= ts:
            current_qty = btc_qty_series[qty_idx][1]
            qty_idx += 1
        spot_values.append(current_qty * float(close))

    if len(spot_values) < 2:
        return 0.0

    peak = spot_values[0]
    max_dd = 0.0
    for v in spot_values:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd * 100.0


def simulate_reinvest(
    trades: list[dict],
    ohlcv: pd.DataFrame,
    reinvest_ratio: float,
) -> dict:
    """FA 거래 결과에서 수익 재투자 시뮬레이션.

    Args:
        trades: FA 백테스트 거래 목록
        ohlcv: OHLCV DataFrame (인덱스=ts)
        reinvest_ratio: 수익 중 BTC 현물 매수 비율 (0.0~1.0)

    Returns:
        dict: 재투자 시뮬레이션 결과
    """
    total_reinvested = 0.0
    spot_btc          = 0.0
    avg_buy_price     = 0.0
    total_cost        = 0.0

    # (timestamp, cumulative_btc_qty) 시계열
    btc_qty_series: list[tuple[pd.Timestamp, float]] = []

    profitable_trades = [t for t in trades if t["pnl"] > 0]

    for trade in profitable_trades:
        pnl = trade["pnl"]
        reinvest_amount = pnl * reinvest_ratio
        if reinvest_amount <= 0:
            continue

        close_ts_str = trade.get("close_ts", "")
        try:
            close_ts = pd.Timestamp(close_ts_str)
            if close_ts.tzinfo is None:
                close_ts = close_ts.tz_localize("UTC")
        except Exception:
            continue

        # 거래 종료 시점 BTC 가격 조회
        mask = ohlcv.index <= close_ts
        if not mask.any():
            continue
        btc_price = float(ohlcv.loc[mask, "close"].iloc[-1])
        if btc_price <= 0:
            continue

        btc_qty = reinvest_amount / btc_price
        spot_btc += btc_qty
        total_reinvested += reinvest_amount
        total_cost += reinvest_amount

        btc_qty_series.append((close_ts, spot_btc))

    # 평균 매수 단가
    if spot_btc > 0 and total_cost > 0:
        avg_buy_price = total_cost / spot_btc
    else:
        avg_buy_price = 0.0

    # 최종 현물 가치 (마지막 OHLCV 종가)
    final_price  = float(ohlcv["close"].iloc[-1])
    spot_value   = spot_btc * final_price
    spot_return  = ((spot_value - total_reinvested) / total_reinvested * 100.0) \
                   if total_reinvested > 0 else 0.0

    # 2022년 구간 현물 MDD
    mdd_2022 = _calc_mdd_2022(ohlcv, btc_qty_series)

    return {
        "reinvest_ratio": reinvest_ratio,
        "total_reinvested": round(total_reinvested, 4),
        "spot_btc": round(spot_btc, 8),
        "avg_buy_price": round(avg_buy_price, 2),
        "spot_value": round(spot_value, 4),
        "spot_return_pct": round(spot_return, 2),
        "mdd_2022_pct": round(mdd_2022, 2),
    }


def _calc_portfolio_return(
    fa_total_profit: float,
    initial_capital: float,
    spot_value: float,
    total_reinvested: float,
) -> float:
    """포트폴리오 총 수익률 계산.

    FA 순수익 + 현물 가치 - 재투자에 사용된 금액 (이미 FA 수익에 포함됨)
    """
    fa_final_equity = initial_capital + fa_total_profit
    # 현물은 FA 수익에서 분리했으므로, 포트폴리오 = FA최종자산(재투자분 제외) + 현물가치
    portfolio_equity = (fa_final_equity - total_reinvested) + spot_value
    return ((portfolio_equity - initial_capital) / initial_capital * 100.0) \
           if initial_capital > 0 else 0.0


# ── DB 저장 ───────────────────────────────────────────────────────────────────

def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


async def save_reinvest_result(
    pool: asyncpg.Pool,
    reinvest_ratio: float,
    sim_result: dict,
    fa_result: dict,
    start: datetime,
    end: datetime,
) -> None:
    variant_name = f"reinvest_{int(reinvest_ratio * 100):02d}pct"
    data_range   = f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}"

    params_json = json.dumps({
        "reinvest_ratio": reinvest_ratio,
        "fa_variant": "short_hold",
        "fa_params": FA_SHORT_HOLD_PARAMS,
        "total_reinvested": _safe_float(sim_result["total_reinvested"]),
        "spot_btc": _safe_float(sim_result["spot_btc"]),
        "avg_buy_price": _safe_float(sim_result["avg_buy_price"]),
        "spot_value": _safe_float(sim_result["spot_value"]),
        "spot_return_pct": _safe_float(sim_result["spot_return_pct"]),
        "mdd_2022_pct": _safe_float(sim_result["mdd_2022_pct"]),
        "fa_total_profit": _safe_float(fa_result["total_profit"]),
        "fa_total_trades": fa_result["total_trades"],
        "fa_win_rate": _safe_float(fa_result["win_rate"]),
    })

    try:
        async with pool.acquire() as conn:
            await conn.execute(CREATE_VARIANT_RESULTS)
            await conn.execute(
                """
                INSERT INTO strategy_variant_results
                    (test_name, variant_name, data_range,
                     total_return, sharpe_ratio, max_drawdown,
                     trade_count, win_rate, profit_factor,
                     monthly_returns, params)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
                """,
                TEST_NAME,
                variant_name,
                data_range,
                _safe_float(sim_result["spot_return_pct"]),
                None,
                _safe_float(sim_result["mdd_2022_pct"]),
                fa_result["total_trades"],
                _safe_float(fa_result["win_rate"]),
                _safe_float(fa_result["profit_factor"], default=0.0),
                "{}",
                params_json,
            )
        log.info("saved", variant=variant_name)
    except Exception as exc:
        log.warning("db_save_failed", variant=variant_name, error=str(exc))


# ── 메인 ──────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
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
    initial_capital = args.initial_capital

    log.info("connecting_db", host=os.getenv("DB_HOST", "postgres"))
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
    except Exception as exc:
        print(f"[ERROR] DB 연결 실패: {exc}")
        print("[HINT]  DB_HOST, DB_PASSWORD 환경변수를 확인하세요.")
        sys.exit(1)

    log.info("loading_data", symbol=SYMBOL, start=args.start, end=args.end)
    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # FA short_hold 백테스트 실행
    log.info("running_fa_short_hold")
    engine = FundingArbShortHoldEngine(
        ohlcv=ohlcv,
        funding=funding,
        params=FA_SHORT_HOLD_PARAMS,
        initial_capital=initial_capital,
        fee_rate=FEE_RATE,
    )
    fa_result = engine.run()
    trades    = fa_result["trades"]

    n_years = (end_dt - start_dt).days / 365.25
    annual_return = _calc_annual_return(fa_result["total_profit_pct"], n_years)

    print()
    print("=" * 70)
    print("=== Stage 0: FA 수익 재투자 시뮬레이션 ===")
    print("=" * 70)
    print()
    print("FA short_hold 6년 결과:")
    print(f"  총 거래: {fa_result['total_trades']}회")
    print(f"  총 수익: ${fa_result['total_profit']:,.2f}")
    print(f"  총 수익률: {fa_result['total_profit_pct']:.2f}%")
    print(f"  연수익률(CAGR): {annual_return:.2f}%")
    print(f"  승률: {fa_result['win_rate']:.1f}%")
    print()

    # 기존 결과 삭제
    try:
        async with pool.acquire() as conn:
            await conn.execute(CREATE_VARIANT_RESULTS)
            deleted = await conn.execute(
                "DELETE FROM strategy_variant_results WHERE test_name = $1",
                TEST_NAME,
            )
            log.info("cleared_previous", deleted=deleted)
    except Exception as exc:
        log.warning("clear_failed", error=str(exc))

    # 재투자 시뮬레이션 실행
    sim_results: list[dict] = []
    for ratio in REINVEST_RATIOS:
        sim = simulate_reinvest(trades, ohlcv, ratio)
        # 포트폴리오 총 수익률 계산
        portfolio_return = _calc_portfolio_return(
            fa_total_profit=fa_result["total_profit"],
            initial_capital=initial_capital,
            spot_value=sim["spot_value"],
            total_reinvested=sim["total_reinvested"],
        )
        sim["portfolio_total_return_pct"] = round(portfolio_return, 2)
        sim_results.append(sim)

        await save_reinvest_result(pool, ratio, sim, fa_result, start_dt, end_dt)

    # 결과 출력 테이블
    header = (
        f"{'reinvest_ratio':>14} | "
        f"{'재투자총액$':>10} | "
        f"{'BTC축적량':>10} | "
        f"{'평균단가$':>10} | "
        f"{'현물가치$':>10} | "
        f"{'현물수익%':>9} | "
        f"{'2022MDD%':>9} | "
        f"{'포트폴리오총수익%':>14}"
    )
    sep = "-" * len(header)
    print(header)
    print(sep)

    for sim in sim_results:
        ratio_str = f"{sim['reinvest_ratio']:.0%}"
        print(
            f"{ratio_str:>14} | "
            f"{sim['total_reinvested']:>10,.2f} | "
            f"{sim['spot_btc']:>10.6f} | "
            f"{sim['avg_buy_price']:>10,.2f} | "
            f"{sim['spot_value']:>10,.2f} | "
            f"{sim['spot_return_pct']:>+8.2f}% | "
            f"{sim['mdd_2022_pct']:>8.2f}% | "
            f"{sim['portfolio_total_return_pct']:>+13.2f}%"
        )

    print(sep)
    print()
    print(f"[DB 저장 완료] test_name='{TEST_NAME}'")
    print()

    await pool.close()
    log.info("stage0_complete")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 0: FA 수익 재투자 시뮬레이션"
    )
    parser.add_argument("--start",           default=START_DATE,
                        help="시작일 YYYY-MM-DD (기본: 2020-04-01)")
    parser.add_argument("--end",             default=END_DATE,
                        help="종료일 YYYY-MM-DD (기본: 2026-03-31)")
    parser.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float,
                        dest="initial_capital",
                        help="초기 자본 USDT (기본: 10000)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
