"""bt_leverage_limit.py — Test 12 Stage C: 레버리지 한계 테스트

FA 전략의 레버리지를 2~10배로 단계적으로 올려서 청산 위험을 시뮬레이션.
2022년 위기 구간 집중 분석 + 편측 체결 리스크 정량화 포함.

실행:
    python tests/backtest/bt_leverage_limit.py
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
import numpy as np
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
log = structlog.get_logger(__name__)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
MAINT_MARGIN    = 0.005   # 0.5%

LEVERAGE_LEVELS = [2, 3, 4, 5, 7, 10]

FA_BASE_PARAMS: dict[str, Any] = {
    "exit_on_flip": True,
    "consecutive_intervals": 3,
    "min_funding_rate": 0.0001,
    "max_hold_bars": 168,
    "fa_capital_ratio": 0.50,
    "reinvest_ratio": 0.30,
}

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS test12_results (
    id      SERIAL PRIMARY KEY,
    run_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage   TEXT NOT NULL,
    variant TEXT NOT NULL,
    metrics JSONB,
    params  JSONB
);
"""


async def load_ohlcv(pool, symbol, timeframe, start, end):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT timestamp AS ts, open, high, low, close, volume "
            "FROM ohlcv_history WHERE symbol=$1 AND timeframe=$2 "
            "AND timestamp>=$3 AND timestamp<=$4 ORDER BY timestamp ASC",
            symbol, timeframe, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    return df


async def load_funding(pool, symbol, start, end):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT timestamp AS ts, rate FROM funding_rate_history "
            "WHERE symbol=$1 AND timestamp>=$2 AND timestamp<=$3 ORDER BY timestamp ASC",
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    df["rate"] = df["rate"].astype(float)
    return df


class FAEngineWithLeverage:
    """FA 엔진 - 레버리지와 청산 시뮬레이션 포함."""

    def __init__(self, ohlcv, funding, params, initial_capital=10_000.0, fee_rate=FEE_RATE):
        self._ohlcv    = ohlcv
        self._funding  = funding
        self._params   = params
        self._capital  = initial_capital
        self._equity   = initial_capital
        self._fee      = fee_rate
        self._equity_curve = [initial_capital]
        self._trades: list[dict] = []
        self._position = None

        # 청산/마진 추적
        self._liquidations: list[dict]     = []
        self._margin_warnings: list[dict]  = []
        self._margin_ratios: list[float]   = []
        self._min_margin_ratio: float      = float("inf")
        self._min_margin_ts: str           = ""
        self._max_unrealized_loss: float   = 0.0

    def run(self) -> dict:
        p        = self._params
        min_rate = p.get("min_funding_rate", 0.0001)
        consec   = p.get("consecutive_intervals", 3)
        max_hold = p.get("max_hold_bars", 168)
        fa_ratio = p.get("fa_capital_ratio", 0.50)
        leverage = p.get("leverage", 2.0)

        bars  = self._ohlcv.reset_index()
        n     = len(bars)
        pos_c = neg_c = 0

        for idx in range(20, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding(bar)
            ts      = bar["ts"]

            try:
                ts_dt = pd.Timestamp(ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.tz_localize("UTC")
                is_settle = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
            except Exception:
                is_settle = (idx % 8 == 0)

            if self._position and is_settle:
                direction = self._position.get("funding_direction", 1)
                # 레버리지 적용: 명목 포지션 = 진입금액 × leverage
                pos_val   = self._position["notional"]
                net_fund  = pos_val * funding * direction
                self._equity += net_fund
                self._position["funding_acc"] = (
                    self._position.get("funding_acc", 0.0) + net_fund
                )

                # 마진 비율 체크 (레버리지 선물 포지션)
                maint    = pos_val * MAINT_MARGIN
                # 베이시스 프록시 기반 미실현 손익 (±)
                basis_change = funding * 8  # 8시간 누적 프록시
                unrealized   = pos_val * basis_change * direction
                if unrealized < self._max_unrealized_loss:
                    self._max_unrealized_loss = unrealized

                margin_avail = self._equity * fa_ratio
                margin_ratio = (margin_avail + unrealized) / maint if maint > 0 else float("inf")
                self._margin_ratios.append(margin_ratio)

                if margin_ratio < self._min_margin_ratio:
                    self._min_margin_ratio = margin_ratio
                    self._min_margin_ts    = str(ts)

                if margin_ratio <= 1.0:
                    self._liquidations.append({
                        "ts": str(ts),
                        "margin_ratio": round(margin_ratio, 4),
                        "leverage": leverage,
                    })
                    self._close(bar)
                    pos_c = neg_c = 0
                    continue

                if margin_ratio <= 1.5:
                    self._margin_warnings.append({
                        "ts": str(ts),
                        "margin_ratio": round(margin_ratio, 4),
                    })

            if self._position is None:
                if is_settle:
                    if funding >= min_rate:
                        pos_c += 1; neg_c = 0
                    elif funding <= -min_rate:
                        neg_c += 1; pos_c = 0
                    else:
                        pos_c = neg_c = 0

                    if pos_c >= consec:
                        self._open(bar, "sell", idx, fa_ratio, leverage)
                        pos_c = 0
                    elif neg_c >= consec:
                        self._open(bar, "buy", idx, fa_ratio, leverage)
                        neg_c = 0
            else:
                direction = self._position.get("funding_direction", 1)
                bars_held = idx - self._position.get("entry_idx", idx)
                rev       = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)

                if is_settle:
                    if rev:
                        self._position["rev_cnt"] = self._position.get("rev_cnt", 0) + 1
                    else:
                        self._position["rev_cnt"] = 0
                    if self._position.get("rev_cnt", 0) >= 3 or bars_held >= max_hold:
                        self._close(bar)
                        pos_c = neg_c = 0
                elif bars_held >= max_hold:
                    self._close(bar)
                    pos_c = neg_c = 0

            self._equity_curve.append(self._equity)

        if self._position:
            self._close(bars.iloc[-1])
            if self._equity_curve:
                self._equity_curve[-1] = self._equity

        return self._build_result()

    def _get_funding(self, bar) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        try:
            ts   = pd.Timestamp(bar["ts"])
            mask = self._funding.index <= ts
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    def _open(self, bar, side, idx, fa_ratio, leverage) -> None:
        entry    = float(bar["close"])
        fa_cap   = self._equity * fa_ratio
        notional = fa_cap * leverage  # 레버리지 적용 명목
        size     = notional / entry
        fee      = notional * self._fee
        self._equity -= fee
        self._position = {
            "side": side,
            "entry_price": entry,
            "size": size,
            "notional": notional,
            "entry_ts": bar["ts"],
            "entry_idx": idx,
            "fee_paid": fee,
            "funding_direction": 1 if side == "sell" else -1,
            "funding_acc": 0.0,
            "rev_cnt": 0,
        }

    def _close(self, bar) -> None:
        if not self._position:
            return
        notional = self._position["notional"]
        fee      = notional * self._fee
        self._equity -= fee
        pnl = self._position.get("funding_acc", 0.0) - self._position.get("fee_paid", 0.0) - fee
        self._trades.append({
            "pnl": pnl,
            "entry_ts": str(self._position.get("entry_ts", "")),
            "close_ts": str(bar["ts"]),
            "notional": notional,
        })
        self._position = None

    def _build_result(self) -> dict:
        total = self._equity - self._capital
        wins  = [t for t in self._trades if t["pnl"] > 0]
        loss  = [t for t in self._trades if t["pnl"] <= 0]
        gp    = sum(t["pnl"] for t in wins)
        gl    = abs(sum(t["pnl"] for t in loss))

        eq  = pd.Series(self._equity_curve)
        mdd = float((eq - eq.cummax()).div(eq.cummax()).min()) * 100

        rets   = eq.pct_change().dropna()
        sharpe = 0.0
        if rets.std() > 0:
            sharpe = float(rets.mean() / rets.std() * math.sqrt(8760))

        avg_margin = float(np.mean(self._margin_ratios)) if self._margin_ratios else 0.0

        return {
            "initial_capital": self._capital,
            "final_equity": round(self._equity, 4),
            "total_profit": round(total, 4),
            "total_profit_pct": round(total / self._capital * 100, 4),
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown_pct": round(mdd, 4),
            "total_trades": len(self._trades),
            "profit_factor": (gp / gl) if gl > 0 else float("inf"),
            "liquidation_count": len(self._liquidations),
            "margin_warning_count": len(self._margin_warnings),
            "min_margin_ratio": round(self._min_margin_ratio, 4)
                                 if self._min_margin_ratio != float("inf") else 9999.0,
            "min_margin_ts": self._min_margin_ts,
            "avg_margin_ratio": round(avg_margin, 4),
            "max_unrealized_loss_usd": round(self._max_unrealized_loss, 4),
        }


def analyze_2022_crisis(ohlcv, funding, leverage, fa_capital_ratio, initial_capital):
    """2022년 5월~7월 위기 구간 마진 비율 분석."""
    s_2022 = pd.Timestamp("2022-05-01", tz="UTC")
    e_2022 = pd.Timestamp("2022-07-31", tz="UTC")

    fund_2022 = funding[(funding.index >= s_2022) & (funding.index <= e_2022)]
    if fund_2022.empty:
        return {"min_margin": 9999.0, "warning_count": 0}

    fa_cap   = initial_capital * fa_capital_ratio
    notional = fa_cap * leverage
    maint    = notional * MAINT_MARGIN

    min_margin  = float("inf")
    warn_count  = 0
    min_margin_ts = ""

    for ts, row in fund_2022.iterrows():
        rate      = float(row["rate"])
        basis_chg = rate * 8
        unrealized = notional * basis_chg  # 숏 방향 기준
        margin_ratio = (fa_cap + unrealized) / maint if maint > 0 else float("inf")

        if margin_ratio < min_margin:
            min_margin    = margin_ratio
            min_margin_ts = str(ts)

        if margin_ratio <= 1.5:
            warn_count += 1

    return {
        "min_margin": round(min_margin, 4) if min_margin != float("inf") else 9999.0,
        "warning_count": warn_count,
        "min_margin_ts": min_margin_ts,
    }


def calc_one_side_fill_risk(ohlcv, leverage_levels):
    """편측 체결 리스크: 3분 내 최대 가격 변동 × 레버리지."""
    # 1h 데이터에서 캔들 내 최대 변동 (high-low)/open 으로 근사
    ohlcv_copy = ohlcv.copy()
    ohlcv_copy["range_pct"] = (ohlcv_copy["high"] - ohlcv_copy["low"]) / ohlcv_copy["open"]

    # 3분은 1h의 5% = 대략 3분/60분 × 최대 캔들 변동의 약 20%로 근사
    max_3min_proxy = ohlcv_copy["range_pct"] * 0.2

    p999 = float(max_3min_proxy.quantile(0.999))

    results = {}
    for lev in leverage_levels:
        worst_pct = p999 * lev * 100
        results[lev] = round(worst_pct, 2)

    return results, round(p999 * 100, 4)


def _safe_float(v, default=0.0):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


async def main(args):
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
    n_years  = (end_dt - start_dt).days / 365.25

    log.info("connecting", host=os.getenv("DB_HOST", "postgres"))
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
    except Exception as exc:
        print(f"[ERROR] DB 연결 실패: {exc}"); sys.exit(1)

    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 없음"); await pool.close(); sys.exit(1)

    log.info("data_loaded", ohlcv=len(ohlcv), funding=len(funding))

    leverage_levels = [int(l) for l in args.leverages.split(",")]

    # 편측 체결 리스크
    one_side_risks, p999_3min = calc_one_side_fill_risk(ohlcv, leverage_levels)

    print()
    print("=" * 80)
    print("  Test 12 Stage C: 레버리지 한계 테스트")
    print("=" * 80)
    print()
    print(f"편측 체결 3분 최대 가격 변동 99.9p: {p999_3min:.4f}%")
    print()

    # DB 초기화
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        await conn.execute("DELETE FROM test12_results WHERE stage='stage_c'")

    all_results = []

    for lev in leverage_levels:
        params = dict(FA_BASE_PARAMS)
        params["leverage"]         = float(lev)
        params["fa_capital_ratio"] = args.fa_capital_ratio

        engine = FAEngineWithLeverage(ohlcv, funding, params, args.initial_capital)
        result = engine.run()

        cagr = ((1 + result["total_profit_pct"] / 100) ** (1 / n_years) - 1) * 100

        # 2022년 위기 분석
        crisis = analyze_2022_crisis(
            ohlcv, funding, float(lev), args.fa_capital_ratio, args.initial_capital
        )

        # 판정
        liq_count = result["liquidation_count"]
        warn_cnt  = result["margin_warning_count"]
        min_mr    = result["min_margin_ratio"]

        if liq_count > 0:
            verdict = "🚫 청산발생"
        elif min_mr < 1.5:
            verdict = "❌ 위험"
        elif min_mr < 2.0:
            verdict = "⚠️ 주의"
        else:
            verdict = "✅ 안전"

        one_side_loss = one_side_risks.get(lev, 0.0)

        row = {
            "leverage": lev,
            "total_profit_pct": result["total_profit_pct"],
            "cagr": round(cagr, 2),
            "sharpe_ratio": result["sharpe_ratio"],
            "max_drawdown_pct": result["max_drawdown_pct"],
            "liquidation_count": liq_count,
            "margin_warning_count": warn_cnt,
            "min_margin_ratio": min_mr,
            "min_margin_ts": result["min_margin_ts"],
            "avg_margin_ratio": result["avg_margin_ratio"],
            "crisis_2022_min_margin": crisis["min_margin"],
            "crisis_2022_warnings": crisis["warning_count"],
            "one_side_loss_pct": one_side_loss,
            "verdict": verdict,
        }
        all_results.append(row)

        metrics = {k: _safe_float(v) if isinstance(v, float) else v for k, v in row.items()}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_c",
                f"leverage_{lev}x",
                json.dumps(metrics),
                json.dumps({"leverage": lev, "fa_capital_ratio": args.fa_capital_ratio,
                            "reinvest_ratio": args.reinvest_ratio}),
            )
        log.info("done", leverage=lev, verdict=verdict, liq=liq_count, min_mr=min_mr)

    # 결과 출력
    header = (
        f"{'레버리지':>7} | {'CAGR%':>6} | {'Sharpe':>7} | {'MDD%':>6} | "
        f"{'청산':>4} | {'마진경고':>6} | {'최소마진':>8} | {'2022최소':>8} | "
        f"{'편측손실%':>8} | {'판정':>10}"
    )
    print(header)
    print("-" * len(header))
    for r in all_results:
        print(
            f"{r['leverage']:>5}x   | "
            f"{r['cagr']:>+5.2f}% | "
            f"{r['sharpe_ratio']:>7.3f} | "
            f"{r['max_drawdown_pct']:>5.2f}% | "
            f"{r['liquidation_count']:>4} | "
            f"{r['margin_warning_count']:>6} | "
            f"{r['min_margin_ratio']:>7.2f}x | "
            f"{r['crisis_2022_min_margin']:>7.2f}x | "
            f"{r['one_side_loss_pct']:>7.2f}% | "
            f"{r['verdict']:>10}"
        )

    print()
    print("통과 기준: 청산0회 + 최소마진≥2.0x + 2022최소마진≥1.5x")

    # 최소마진 시점 상세
    print()
    print("=== 최소 마진비율 발생 시점 ===")
    for r in all_results:
        if r["min_margin_ts"]:
            print(f"  {r['leverage']}x: {r['min_margin_ts']} (마진비율={r['min_margin_ratio']:.2f}x)")

    print()
    print("=== 편측 체결 리스크 ===")
    print(f"3분 내 최대 가격 변동 99.9퍼센타일: {p999_3min:.4f}%")
    for lev, loss in one_side_risks.items():
        print(f"  레버리지 {lev}x: 편측 체결 최악 손실 = 자본의 {loss:.2f}%")

    print(f"\n[DB 저장 완료] stage=stage_c, {len(all_results)}개 변형")

    await pool.close()
    return all_results


def _parse():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=START_DATE)
    p.add_argument("--end",   default=END_DATE)
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, dest="initial_capital")
    p.add_argument("--fa-capital-ratio", default=0.50, type=float, dest="fa_capital_ratio")
    p.add_argument("--leverages", default="2,3,4,5,7,10")
    p.add_argument("--reinvest-ratio", default=0.30, type=float, dest="reinvest_ratio")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 중단"); sys.exit(0)
