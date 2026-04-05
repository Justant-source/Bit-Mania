"""bt_fa80_extended.py — Test 12 Stage D2: FA80 확장 조합 탐색

Stage D에서 커버하지 않은 FA80 + 레버리지 3~5배 × 재투자 30/50/70/90% 조합을
체계적으로 탐색한다. FA90 재투자 비율 확장도 비교군으로 포함.

실행:
    python tests/backtest/combined/bt_fa80_extended.py
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

import asyncpg
import pandas as pd
import structlog

log = structlog.get_logger(__name__)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
MAINT_MARGIN    = 0.005

# (FA비율, 레버리지, 재투자비율, 이름)
# Stage D 기준점 (비교용)
REFERENCE_COMBINATIONS = [
    (0.80, 3, 0.50, "fa80_lev3_r50_ref"),   # Stage D 결과: +22.61%
    (0.90, 3, 0.30, "fa90_lev3_r30_ref"),   # Stage D 최적: +24.57%
]

# FA80 확장 조합
FA80_COMBINATIONS = [
    # lev3 × 재투자 전 범위
    (0.80, 3, 0.30, "fa80_lev3_r30"),
    (0.80, 3, 0.50, "fa80_lev3_r50"),
    (0.80, 3, 0.70, "fa80_lev3_r70"),
    (0.80, 3, 0.90, "fa80_lev3_r90"),
    # lev4 × 재투자 다양화
    (0.80, 4, 0.30, "fa80_lev4_r30"),
    (0.80, 4, 0.50, "fa80_lev4_r50"),
    (0.80, 4, 0.70, "fa80_lev4_r70"),
    (0.80, 4, 0.90, "fa80_lev4_r90"),
    # lev5 (고수익 탐색)
    (0.80, 5, 0.30, "fa80_lev5_r30"),
    (0.80, 5, 0.50, "fa80_lev5_r50"),
    (0.80, 5, 0.70, "fa80_lev5_r70"),
    (0.80, 5, 0.90, "fa80_lev5_r90"),
]

# FA90 재투자 비율 확장 (Stage D에서 30%만 테스트됨)
FA90_COMBINATIONS = [
    (0.90, 3, 0.30, "fa90_lev3_r30"),
    (0.90, 3, 0.50, "fa90_lev3_r50"),
    (0.90, 3, 0.70, "fa90_lev3_r70"),
    (0.90, 3, 0.90, "fa90_lev3_r90"),
]

ALL_COMBINATIONS = REFERENCE_COMBINATIONS + FA80_COMBINATIONS + FA90_COMBINATIONS

FA_BASE_PARAMS = {
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
    """FA 엔진 — fa_capital_ratio, leverage, reinvest_ratio 통합."""

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
        notional = self._position["notional"]
        fee      = notional * self._fee
        self._equity -= fee
        pnl = self._position.get("funding_acc", 0.0) - self._position.get("fee_paid", 0.0) - fee

        if pnl > 0 and reinv_ratio > 0:
            reinv_amt  = pnl * reinv_ratio
            btc_price  = float(bar["close"])
            if btc_price > 0:
                btc_qty    = reinv_amt / btc_price
                self._spot_btc        += btc_qty
                self._total_reinvested += reinv_amt
                self._equity           -= reinv_amt

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

        final_price      = float(self._ohlcv["close"].iloc[-1])
        spot_value       = self._spot_btc * final_price
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
            "liquidation_count": self._liquidation_count,
            "margin_warning_count": self._margin_warnings,
            "min_margin_ratio": round(min_mr, 4),
            "min_margin_ts": self._min_margin_ts,
        }


def _safe_float(v, default=0.0):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


def _verdict(liq, mr):
    if liq > 0:
        return "🚫"
    if mr < 1.5:
        return "❌"
    if mr < 2.0:
        return "⚠️"
    return "✅"


async def run_combinations(pool, ohlcv, funding, combos, stage_tag, n_years, initial_capital):
    results = []
    for fa_ratio, lev, reinv, name in combos:
        params = dict(FA_BASE_PARAMS)
        params["fa_capital_ratio"] = fa_ratio
        params["leverage"]         = float(lev)
        params["reinvest_ratio"]   = reinv

        engine = FAFullEngine(ohlcv, funding, params, initial_capital)
        result = engine.run()

        cagr    = ((1 + result["portfolio_ret_pct"] / 100) ** (1 / n_years) - 1) * 100
        liq     = result["liquidation_count"]
        mr      = result["min_margin_ratio"]
        verdict = _verdict(liq, mr)
        ann_1k  = round(cagr * 10, 2)

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
            "min_margin_ratio": mr,
            "verdict": verdict,
            "annual_per_1k": ann_1k,
        }
        results.append(row)

        metrics = {k: _safe_float(v) if isinstance(v, float) else v
                   for k, v in {**row, **result}.items()}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                stage_tag, name,
                json.dumps(metrics),
                json.dumps({"fa_ratio": fa_ratio, "leverage": lev, "reinvest_ratio": reinv}),
            )
        log.info("done", name=name, cagr=round(cagr, 2), verdict=verdict)

    return results


def _print_table(title, results):
    print()
    print("=" * 100)
    print(f"  {title}")
    print("=" * 100)
    header = (
        f"{'조합':>22} | {'FA%':>4} | {'Lev':>3} | {'Reinv%':>6} | "
        f"{'연수익%':>7} | {'Sharpe':>7} | {'MDD%':>6} | "
        f"{'청산':>4} | {'최소마진':>8} | {'$1K연수익':>9} | {'판정':>6}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['name']:>22} | "
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


async def main(args):
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
        await conn.execute("DELETE FROM test12_results WHERE stage='stage_d2'")

    print()
    print("=" * 100)
    print("  Test 12 Stage D2: FA80 확장 조합 탐색")
    print(f"  데이터: {args.start} ~ {args.end}  |  초기자본: ${args.initial_capital:,.0f}")
    print("=" * 100)

    # 1) 기준점 (Stage D 재현)
    log.info("running_reference_combinations")
    ref_results = await run_combinations(
        pool, ohlcv, funding, REFERENCE_COMBINATIONS, "stage_d2", n_years, args.initial_capital
    )
    _print_table("기준점 (Stage D 결과 재현)", ref_results)

    # 2) FA80 확장
    log.info("running_fa80_combinations")
    fa80_results = await run_combinations(
        pool, ohlcv, funding, FA80_COMBINATIONS, "stage_d2", n_years, args.initial_capital
    )
    _print_table("FA80 확장: 레버리지 3~5x × 재투자 30~90%", fa80_results)

    # 3) FA90 재투자 확장
    log.info("running_fa90_combinations")
    fa90_results = await run_combinations(
        pool, ohlcv, funding, FA90_COMBINATIONS, "stage_d2", n_years, args.initial_capital
    )
    _print_table("FA90 재투자 확장: 레버리지 3x × 재투자 30~90%", fa90_results)

    # 전체 통합 분석
    all_results = ref_results + fa80_results + fa90_results

    valid = [r for r in all_results
             if r["liquidation_count"] == 0 and r["min_margin_ratio"] >= 2.0]

    print()
    print("=" * 100)
    print("  종합 최적 조합 분석 (청산 0회 + 최소마진 ≥ 2.0x)")
    print("=" * 100)

    if valid:
        valid_sorted = sorted(valid, key=lambda x: x["cagr"], reverse=True)
        print(f"\n  ▶ 유효 조합 수: {len(valid)}개 (전체 {len(all_results)}개 중)")
        print()
        print("  [TOP 10 — 연수익 기준]")
        for i, r in enumerate(valid_sorted[:10], 1):
            print(
                f"  {i:>2}. {r['name']:<25} FA={r['fa_ratio']:.0%} "
                f"Lev={r['leverage']}x Reinv={r['reinvest_ratio']:.0%} "
                f"→ 연수익 {r['cagr']:+.2f}% | Sharpe {r['sharpe_ratio']:.3f} "
                f"| MDD {r['max_drawdown_pct']:.2f}% | 최소마진 {r['min_margin_ratio']:.1f}x"
            )

        # Sharpe 상위
        sharpe_sorted = sorted(valid, key=lambda x: x["sharpe_ratio"], reverse=True)
        print()
        print("  [TOP 5 — Sharpe 기준]")
        for i, r in enumerate(sharpe_sorted[:5], 1):
            print(
                f"  {i:>2}. {r['name']:<25} Sharpe={r['sharpe_ratio']:.3f} "
                f"→ 연수익 {r['cagr']:+.2f}% | MDD {r['max_drawdown_pct']:.2f}%"
            )

        # 레버리지별 최적
        print()
        print("  [레버리지별 최적 조합]")
        for lev in [2, 3, 4, 5]:
            lev_valid = [r for r in valid if r["leverage"] == lev]
            if lev_valid:
                best = max(lev_valid, key=lambda x: x["cagr"])
                print(
                    f"  Lev {lev}x 최적: {best['name']:<25} "
                    f"연수익 {best['cagr']:+.2f}% | Sharpe {best['sharpe_ratio']:.3f} "
                    f"| MDD {best['max_drawdown_pct']:.2f}% | 최소마진 {best['min_margin_ratio']:.1f}x"
                )

        # FA80 vs FA90 비교 (lev3 기준)
        print()
        print("  [FA80 vs FA90 — 레버리지 3x, 재투자별 수익 비교]")
        print(f"  {'재투자':>8} | {'FA80 연수익':>11} | {'FA80 MDD':>9} | {'FA90 연수익':>11} | {'FA90 MDD':>9}")
        print("  " + "-" * 60)
        for reinv in [0.30, 0.50, 0.70, 0.90]:
            r80 = next((r for r in all_results
                        if r["fa_ratio"] == 0.80 and r["leverage"] == 3 and r["reinvest_ratio"] == reinv), None)
            r90 = next((r for r in all_results
                        if r["fa_ratio"] == 0.90 and r["leverage"] == 3 and r["reinvest_ratio"] == reinv), None)
            c80 = f"{r80['cagr']:+.2f}%{r80['verdict']}" if r80 else "-"
            m80 = f"{r80['max_drawdown_pct']:.2f}%" if r80 else "-"
            c90 = f"{r90['cagr']:+.2f}%{r90['verdict']}" if r90 else "-"
            m90 = f"{r90['max_drawdown_pct']:.2f}%" if r90 else "-"
            print(f"  {reinv:>7.0%}  | {c80:>11} | {m80:>9} | {c90:>11} | {m90:>9}")

    else:
        print("  [WARNING] 청산 0 + 최소마진≥2 조건 만족 조합 없음")

    # 최적 조합 DB 저장
    if valid:
        overall_best = max(valid, key=lambda x: x["cagr"])
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_d2_optimal",
                overall_best["name"],
                json.dumps({k: _safe_float(v) if isinstance(v, float) else v
                            for k, v in overall_best.items()}),
                json.dumps({"selected": True,
                            "fa_ratio": overall_best["fa_ratio"],
                            "leverage": overall_best["leverage"],
                            "reinvest_ratio": overall_best["reinvest_ratio"]}),
            )

    print(f"\n[DB 저장 완료] stage=stage_d2, {len(all_results)}개 조합")
    await pool.close()


def _parse():
    p = argparse.ArgumentParser()
    p.add_argument("--start",           default=START_DATE)
    p.add_argument("--end",             default=END_DATE)
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, dest="initial_capital")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 중단"); sys.exit(0)
