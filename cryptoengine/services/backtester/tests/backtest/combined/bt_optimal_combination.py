"""bt_optimal_combination.py — Test 12 Stage D: 최적 조합 탐색

Stage A~C의 결과를 조합하여 수익 극대화 + 청산 안전 최적점 탐색.
11가지 (FA비율, 레버리지, 재투자비율) 조합을 교차 테스트.

실행:
    python tests/backtest/bt_optimal_combination.py
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

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
MAINT_MARGIN    = 0.005

# (FA비율, 레버리지, 재투자비율, 이름)
COMBINATIONS = [
    (0.50, 2, 0.30, "baseline"),
    (0.50, 2, 0.50, "reinvest_50"),
    (0.50, 2, 0.70, "reinvest_70"),
    (0.70, 2, 0.30, "fa70_baseline"),
    (0.70, 2, 0.50, "fa70_reinvest50"),
    (0.70, 3, 0.30, "fa70_lev3"),
    (0.70, 3, 0.50, "fa70_lev3_r50"),
    (0.80, 2, 0.50, "fa80_r50"),
    (0.80, 3, 0.50, "fa80_lev3_r50"),
    (0.90, 2, 0.50, "fa90_r50"),
    (0.90, 3, 0.30, "fa90_lev3"),
]

FA_BASE_PARAMS: dict[str, Any] = {
    "exit_on_flip": True,
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


class FAFullEngine:
    """FA 엔진 - 모든 파라미터(fa_capital_ratio, leverage, reinvest_ratio) 통합."""

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
        self._spot_btc = 0.0
        self._total_reinvested = 0.0
        self._liquidation_count = 0
        self._margin_warnings   = 0
        self._min_margin_ratio  = float("inf")
        self._min_margin_ts     = ""

    def run(self) -> dict:
        p           = self._params
        min_rate    = p.get("min_funding_rate", 0.0001)
        consec      = p.get("consecutive_intervals", 3)
        max_hold    = p.get("max_hold_bars", 168)
        fa_ratio    = p.get("fa_capital_ratio", 0.50)
        leverage    = p.get("leverage", 2.0)
        reinv_ratio = p.get("reinvest_ratio", 0.30)

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
                notional  = self._position["notional"]
                net_fund  = notional * funding * direction
                self._equity += net_fund
                self._position["funding_acc"] = self._position.get("funding_acc", 0.0) + net_fund

                # 마진 체크
                maint        = notional * MAINT_MARGIN
                basis_chg    = funding * 8
                unrealized   = notional * basis_chg * direction
                margin_avail = self._equity * fa_ratio
                margin_ratio = (margin_avail + unrealized) / maint if maint > 0 else 9999.0

                if margin_ratio < self._min_margin_ratio:
                    self._min_margin_ratio = margin_ratio
                    self._min_margin_ts    = str(ts)

                if margin_ratio <= 1.0:
                    self._liquidation_count += 1
                    self._close_with_reinvest(bar, reinv_ratio)
                    pos_c = neg_c = 0
                    continue
                if margin_ratio <= 1.5:
                    self._margin_warnings += 1

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
                        self._close_with_reinvest(bar, reinv_ratio)
                        pos_c = neg_c = 0
                elif bars_held >= max_hold:
                    self._close_with_reinvest(bar, reinv_ratio)
                    pos_c = neg_c = 0

            self._equity_curve.append(self._equity)

        if self._position:
            self._close_with_reinvest(bars.iloc[-1], reinv_ratio)
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
        notional = fa_cap * leverage
        fee      = notional * self._fee
        self._equity -= fee
        self._position = {
            "side": side,
            "entry_price": entry,
            "notional": notional,
            "entry_ts": bar["ts"],
            "entry_idx": idx,
            "fee_paid": fee,
            "funding_direction": 1 if side == "sell" else -1,
            "funding_acc": 0.0,
            "rev_cnt": 0,
        }

    def _close_with_reinvest(self, bar, reinv_ratio) -> None:
        if not self._position:
            return
        notional  = self._position["notional"]
        fee       = notional * self._fee
        self._equity -= fee
        pnl = self._position.get("funding_acc", 0.0) - self._position.get("fee_paid", 0.0) - fee

        # 수익 재투자 (현물 BTC 매수)
        if pnl > 0 and reinv_ratio > 0:
            reinv_amt  = pnl * reinv_ratio
            btc_price  = float(bar["close"])
            if btc_price > 0:
                btc_qty    = reinv_amt / btc_price
                self._spot_btc        += btc_qty
                self._total_reinvested += reinv_amt
                self._equity           -= reinv_amt  # FA equity에서 차감

        self._trades.append({
            "pnl": pnl,
            "entry_ts": str(self._position.get("entry_ts", "")),
            "close_ts": str(bar["ts"]),
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

        # 현물 최종 가치
        final_price = float(self._ohlcv["close"].iloc[-1])
        spot_value  = self._spot_btc * final_price

        # 포트폴리오 총 수익률 = FA equity + 현물 가치
        portfolio_equity = self._equity + spot_value
        portfolio_ret    = (portfolio_equity - self._capital) / self._capital * 100

        min_mr = self._min_margin_ratio if self._min_margin_ratio != float("inf") else 9999.0

        return {
            "initial_capital": self._capital,
            "final_equity": round(self._equity, 4),
            "fa_total_profit_pct": round(total / self._capital * 100, 4),
            "spot_btc": round(self._spot_btc, 8),
            "spot_value": round(spot_value, 4),
            "total_reinvested": round(self._total_reinvested, 4),
            "portfolio_ret_pct": round(portfolio_ret, 4),
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown_pct": round(mdd, 4),
            "total_trades": len(self._trades),
            "profit_factor": (gp / gl) if gl > 0 else float("inf"),
            "liquidation_count": self._liquidation_count,
            "margin_warning_count": self._margin_warnings,
            "min_margin_ratio": round(min_mr, 4),
            "min_margin_ts": self._min_margin_ts,
        }


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

    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        await conn.execute("DELETE FROM test12_results WHERE stage='stage_d'")

    print()
    print("=" * 90)
    print("  Test 12 Stage D: 최적 조합 탐색")
    print("=" * 90)
    print()

    all_results = []

    for fa_ratio, lev, reinv, name in COMBINATIONS:
        params = dict(FA_BASE_PARAMS)
        params["fa_capital_ratio"] = fa_ratio
        params["leverage"]         = float(lev)
        params["reinvest_ratio"]   = reinv

        engine = FAFullEngine(ohlcv, funding, params, args.initial_capital)
        result = engine.run()

        cagr = ((1 + result["portfolio_ret_pct"] / 100) ** (1 / n_years) - 1) * 100

        # 판정
        liq = result["liquidation_count"]
        mr  = result["min_margin_ratio"]

        if liq > 0:
            verdict = "🚫"
        elif mr < 1.5:
            verdict = "❌"
        elif mr < 2.0:
            verdict = "⚠️"
        else:
            verdict = "✅"

        # $1,000 투자 시 연수익
        annual_per_1k = cagr * 10  # $1,000 × CAGR%

        row = {
            "name": name,
            "fa_ratio": fa_ratio,
            "leverage": lev,
            "reinvest_ratio": reinv,
            "portfolio_ret_pct": result["portfolio_ret_pct"],
            "cagr": round(cagr, 2),
            "sharpe_ratio": result["sharpe_ratio"],
            "max_drawdown_pct": result["max_drawdown_pct"],
            "liquidation_count": liq,
            "margin_warning_count": result["margin_warning_count"],
            "min_margin_ratio": mr,
            "fa_total_profit_pct": result["fa_total_profit_pct"],
            "spot_value": result["spot_value"],
            "verdict": verdict,
            "annual_per_1k": round(annual_per_1k, 2),
        }
        all_results.append(row)

        metrics = {k: _safe_float(v) if isinstance(v, float) else v for k, v in {**row, **result}.items()}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_d", name,
                json.dumps(metrics),
                json.dumps({"fa_ratio": fa_ratio, "leverage": lev, "reinvest_ratio": reinv}),
            )
        log.info("done", name=name, cagr=cagr, verdict=verdict)

    # 결과 출력
    header = (
        f"{'조합':>16} | {'FA%':>4} | {'Lev':>3} | {'Reinv%':>6} | "
        f"{'연수익%':>7} | {'Sharpe':>7} | {'MDD%':>6} | "
        f"{'청산':>4} | {'최소마진':>8} | {'$1K연수익$':>9} | {'판정':>6}"
    )
    print(header)
    print("-" * len(header))
    for r in all_results:
        print(
            f"{r['name']:>16} | "
            f"{r['fa_ratio']:>3.0%} | "
            f"{r['leverage']:>3}x | "
            f"{r['reinvest_ratio']:>5.0%} | "
            f"{r['cagr']:>+6.2f}% | "
            f"{r['sharpe_ratio']:>7.3f} | "
            f"{r['max_drawdown_pct']:>5.2f}% | "
            f"{r['liquidation_count']:>4} | "
            f"{r['min_margin_ratio']:>7.2f}x | "
            f"${r['annual_per_1k']:>8.2f} | "
            f"{r['verdict']:>6}"
        )

    # 최적점 선택 (청산 0, 마진≥2, 연수익 최대)
    valid = [r for r in all_results
             if r["liquidation_count"] == 0 and r["min_margin_ratio"] >= 2.0]
    if valid:
        optimal = max(valid, key=lambda x: x["cagr"])
        aggressive = max(valid, key=lambda x: x["cagr"])  # 동일 기준

        print()
        print("=" * 60)
        print("  최적 조합 선택 결과")
        print("=" * 60)
        print(f"  조건: 청산 0회, 최소마진비율 ≥ 2.0x")
        print(f"  최적: {optimal['name']}")
        print(f"    FA비율={optimal['fa_ratio']:.0%}, "
              f"레버리지={optimal['leverage']}x, "
              f"재투자={optimal['reinvest_ratio']:.0%}")
        print(f"    연수익={optimal['cagr']:.2f}%, "
              f"Sharpe={optimal['sharpe_ratio']:.3f}, "
              f"MDD={optimal['max_drawdown_pct']:.2f}%")
        print(f"    $1,000 투자 시 연간 ${optimal['annual_per_1k']:.2f}")

        # DB에 최적 조합 저장
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_d_optimal",
                optimal["name"],
                json.dumps({k: _safe_float(v) if isinstance(v, float) else v
                            for k, v in optimal.items()}),
                json.dumps({"selected": True,
                            "fa_ratio": optimal["fa_ratio"],
                            "leverage": optimal["leverage"],
                            "reinvest_ratio": optimal["reinvest_ratio"]}),
            )
    else:
        print("\n[WARNING] 청산 0 + 최소마진≥2 조건을 만족하는 조합 없음!")
        print("  마진 조건을 ≥1.5로 완화하여 재탐색:")
        valid_relaxed = [r for r in all_results
                         if r["liquidation_count"] == 0 and r["min_margin_ratio"] >= 1.5]
        if valid_relaxed:
            optimal = max(valid_relaxed, key=lambda x: x["cagr"])
            print(f"  완화 조건 최적: {optimal['name']} (CAGR={optimal['cagr']:.2f}%)")

    print(f"\n[DB 저장 완료] stage=stage_d, {len(all_results)}개 조합")

    await pool.close()
    return all_results


def _parse():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=START_DATE)
    p.add_argument("--end",   default=END_DATE)
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, dest="initial_capital")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 중단"); sys.exit(0)
