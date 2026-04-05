"""bt_reinvest_optimization.py — Test 12 Stage A: 재투자 비율 최적화

FA short_hold 6년 백테스트 후 수익 재투자 비율을 7가지로 변경.
연도별 BTC 축적 추적 포함.

실행:
    python tests/backtest/bt_reinvest_optimization.py
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

log = structlog.get_logger(__name__)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
TEST_NAME       = "test_12_stage_a"

REINVEST_RATIOS = [0.0, 0.30, 0.50, 0.60, 0.70, 0.80, 1.00]

FA_PARAMS: dict[str, Any] = {
    "exit_on_flip": True,
    "negative_hours_before_exit": 0,
    "consecutive_intervals": 3,
    "min_funding_rate": 0.0001,
    "max_hold_bars": 168,
    "fa_capital_ratio": 0.50,
    "leverage": 2.0,
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
    id           SERIAL PRIMARY KEY,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage        TEXT NOT NULL,
    variant      TEXT NOT NULL,
    metrics      JSONB,
    params       JSONB
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


class FAEngine:
    """FA short_hold 백테스트 엔진 (fa_capital_ratio, leverage 지원)."""

    def __init__(self, ohlcv, funding, params, initial_capital=10_000.0, fee_rate=FEE_RATE):
        self._ohlcv   = ohlcv
        self._funding = funding
        self._params  = params
        self._capital = initial_capital
        self._equity  = initial_capital
        self._fee     = fee_rate
        self._equity_curve = [initial_capital]
        self._trades: list[dict] = []
        self._position = None

    def run(self) -> dict:
        p          = self._params
        min_rate   = p.get("min_funding_rate", 0.0001)
        consec     = p.get("consecutive_intervals", 3)
        max_hold   = p.get("max_hold_bars", 168)
        fa_ratio   = p.get("fa_capital_ratio", 0.50)

        bars = self._ohlcv.reset_index()
        n    = len(bars)
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

            # 펀딩비 정산
            if self._position and is_settle:
                direction = self._position.get("funding_direction", 1)
                pos_val   = self._position["size"] * self._position["entry_price"]
                net_fund  = pos_val * funding * direction
                self._equity += net_fund
                self._position["funding_acc"] = (
                    self._position.get("funding_acc", 0.0) + net_fund
                )

            if self._position is None:
                if is_settle:
                    if funding >= min_rate:
                        pos_c += 1; neg_c = 0
                    elif funding <= -min_rate:
                        neg_c += 1; pos_c = 0
                    else:
                        pos_c = neg_c = 0

                    if pos_c >= consec:
                        self._open(bar, "sell", idx, fa_ratio)
                        pos_c = 0
                    elif neg_c >= consec:
                        self._open(bar, "buy", idx, fa_ratio)
                        neg_c = 0
            else:
                direction   = self._position.get("funding_direction", 1)
                bars_held   = idx - self._position.get("entry_idx", idx)
                rev         = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)
                close_pos   = False

                if is_settle:
                    if rev:
                        self._position["rev_cnt"] = self._position.get("rev_cnt", 0) + 1
                    else:
                        self._position["rev_cnt"] = 0
                    if self._position.get("rev_cnt", 0) >= 3:
                        close_pos = True

                if bars_held >= max_hold:
                    close_pos = True

                if close_pos:
                    self._close(bar)
                    pos_c = neg_c = 0

            self._equity_curve.append(self._equity)

        if self._position is not None:
            self._close(bars.iloc[-1])
            if self._equity_curve:
                self._equity_curve[-1] = self._equity

        return self._build_result()

    def _get_funding(self, bar) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        ts = bar["ts"]
        try:
            ts_pd = pd.Timestamp(ts)
            mask  = self._funding.index <= ts_pd
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    def _open(self, bar, side, idx, fa_ratio) -> None:
        entry = float(bar["close"])
        size  = (self._equity * fa_ratio) / entry
        fee   = entry * size * self._fee
        self._equity -= fee
        self._position = {
            "side": side,
            "entry_price": entry,
            "size": size,
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
        size       = self._position["size"]
        exit_price = float(bar["close"])
        fee_exit   = exit_price * size * self._fee
        self._equity -= fee_exit
        net_pnl = (
            self._position.get("funding_acc", 0.0)
            - self._position.get("fee_paid", 0.0)
            - fee_exit
        )
        self._trades.append({
            "pnl": net_pnl,
            "funding_acc": self._position.get("funding_acc", 0.0),
            "entry_ts": str(self._position.get("entry_ts", "")),
            "close_ts": str(bar["ts"]),
            "entry_price": self._position["entry_price"],
            "exit_price": exit_price,
            "size": size,
        })
        self._position = None

    def _build_result(self) -> dict:
        total = self._equity - self._capital
        wins  = [t for t in self._trades if t["pnl"] > 0]
        loss  = [t for t in self._trades if t["pnl"] <= 0]
        gp    = sum(t["pnl"] for t in wins)
        gl    = abs(sum(t["pnl"] for t in loss))

        # MDD
        eq = pd.Series(self._equity_curve)
        roll_max = eq.cummax()
        dd_series = (eq - roll_max) / roll_max
        mdd = float(dd_series.min()) * 100

        # Sharpe
        rets = eq.pct_change().dropna()
        sharpe = 0.0
        if rets.std() > 0:
            sharpe = float(rets.mean() / rets.std() * math.sqrt(8760))

        return {
            "initial_capital": self._capital,
            "final_equity": round(self._equity, 4),
            "total_profit": round(total, 4),
            "total_profit_pct": round(total / self._capital * 100, 4),
            "win_rate": round(len(wins) / len(self._trades) * 100, 2) if self._trades else 0.0,
            "total_trades": len(self._trades),
            "profit_factor": (gp / gl) if gl > 0 else float("inf"),
            "max_drawdown_pct": round(mdd, 4),
            "sharpe_ratio": round(sharpe, 4),
            "equity_curve": self._equity_curve,
            "trades": self._trades,
        }


def simulate_reinvest(trades, ohlcv, reinvest_ratio):
    spot_btc = 0.0
    total_reinvested = 0.0
    btc_qty_series = []

    for t in trades:
        if t["pnl"] <= 0:
            continue
        amt = t["pnl"] * reinvest_ratio
        if amt <= 0:
            continue
        try:
            ts = pd.Timestamp(t["close_ts"])
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
        except Exception:
            continue
        mask = ohlcv.index <= ts
        if not mask.any():
            continue
        price = float(ohlcv.loc[mask, "close"].iloc[-1])
        if price <= 0:
            continue
        qty = amt / price
        spot_btc += qty
        total_reinvested += amt
        btc_qty_series.append((ts, spot_btc))

    final_price = float(ohlcv["close"].iloc[-1])
    spot_value  = spot_btc * final_price
    avg_cost    = (total_reinvested / spot_btc) if spot_btc > 0 else 0.0
    spot_ret    = ((spot_value - total_reinvested) / total_reinvested * 100) if total_reinvested > 0 else 0.0

    # 2022년 MDD
    mdd_2022 = _calc_spot_mdd(ohlcv, btc_qty_series, "2022-01-01", "2022-12-31")

    # 전체 구간 현물 MDD
    mdd_full = _calc_spot_mdd(ohlcv, btc_qty_series, START_DATE, END_DATE)

    return {
        "total_reinvested": round(total_reinvested, 2),
        "spot_btc": round(spot_btc, 8),
        "avg_cost": round(avg_cost, 2),
        "spot_value": round(spot_value, 2),
        "spot_return_pct": round(spot_ret, 2),
        "mdd_2022_pct": round(mdd_2022, 2),
        "mdd_full_pct": round(mdd_full, 2),
        "btc_qty_series": btc_qty_series,
    }


def _calc_spot_mdd(ohlcv, btc_qty_series, start_str, end_str):
    s = pd.Timestamp(start_str, tz="UTC")
    e = pd.Timestamp(end_str,   tz="UTC")
    df = ohlcv[(ohlcv.index >= s) & (ohlcv.index <= e)]
    if df.empty or not btc_qty_series:
        return 0.0
    cur_qty = 0.0
    qi = 0
    vals = []
    for ts, close in df["close"].items():
        while qi < len(btc_qty_series) and btc_qty_series[qi][0] <= ts:
            cur_qty = btc_qty_series[qi][1]
            qi += 1
        vals.append(cur_qty * float(close))
    if len(vals) < 2:
        return 0.0
    peak = mdd = 0.0
    for v in vals:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd
    return mdd * 100.0


def track_yearly_accumulation(trades, ohlcv, reinvest_ratio):
    """연도별 FA 수익, 재투자액, BTC 매수량, 연말 BTC 가치 추적."""
    years = list(range(2020, 2027))
    rows  = []

    cum_btc = 0.0
    cum_reinvested = 0.0

    for year in years:
        y_start = pd.Timestamp(f"{year}-01-01", tz="UTC")
        y_end   = pd.Timestamp(f"{year}-12-31 23:59:59", tz="UTC")

        year_trades = [
            t for t in trades
            if t["pnl"] > 0 and _in_range(t["close_ts"], y_start, y_end)
        ]
        year_fa_pnl = sum(t["pnl"] for t in trades if _in_range(t["close_ts"], y_start, y_end))
        year_reinvest = sum(t["pnl"] * reinvest_ratio for t in year_trades)

        # 해당 연도 BTC 매수
        year_btc = 0.0
        for t in year_trades:
            amt = t["pnl"] * reinvest_ratio
            try:
                ts = pd.Timestamp(t["close_ts"])
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
            except Exception:
                continue
            mask = ohlcv.index <= ts
            if not mask.any():
                continue
            price = float(ohlcv.loc[mask, "close"].iloc[-1])
            if price > 0:
                year_btc += amt / price

        cum_btc += year_btc
        cum_reinvested += year_reinvest

        # 연말 BTC 가격
        mask = ohlcv.index <= y_end
        year_end_price = float(ohlcv.loc[mask, "close"].iloc[-1]) if mask.any() else 0.0
        spot_val = cum_btc * year_end_price
        spot_ret = ((spot_val - cum_reinvested) / cum_reinvested * 100) if cum_reinvested > 0 else 0.0

        rows.append({
            "year":          year,
            "fa_pnl_usd":    round(year_fa_pnl, 2),
            "reinvested_usd": round(year_reinvest, 2),
            "btc_bought":    round(year_btc, 6),
            "year_end_price": round(year_end_price, 0),
            "cum_btc":        round(cum_btc, 6),
            "spot_value_usd": round(spot_val, 2),
            "spot_return_pct": round(spot_ret, 2),
        })
    return rows


def _in_range(ts_str, s, e):
    try:
        ts = pd.Timestamp(ts_str)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return s <= ts <= e
    except Exception:
        return False


def _safe_float(v, default=0.0):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


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
        print(f"[ERROR] DB 연결 실패: {exc}")
        sys.exit(1)

    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 없음")
        await pool.close()
        sys.exit(1)

    log.info("data_loaded", ohlcv=len(ohlcv), funding=len(funding))

    # FA 백테스트 실행
    engine = FAEngine(ohlcv, funding, FA_PARAMS, args.initial_capital)
    fa     = engine.run()
    trades = fa["trades"]

    fa_cagr = ((1 + fa["total_profit_pct"] / 100) ** (1 / n_years) - 1) * 100

    # 결과 테이블 초기화
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        await conn.execute("DELETE FROM test12_results WHERE stage='stage_a'")

    print()
    print("=" * 80)
    print("  Test 12 Stage A: 재투자 비율 최적화")
    print("=" * 80)
    print(f"  FA 기본 성과: 수익률={fa['total_profit_pct']:.2f}%  CAGR={fa_cagr:.2f}%  "
          f"Sharpe={fa['sharpe_ratio']:.3f}  MDD={fa['max_drawdown_pct']:.2f}%  거래={fa['total_trades']}")
    print()

    all_results = []
    for ratio in REINVEST_RATIOS:
        sim = simulate_reinvest(trades, ohlcv, ratio)

        # 포트폴리오 수익률
        fa_equity_no_reinvest = fa["final_equity"] - sim["total_reinvested"]
        portfolio_equity      = fa_equity_no_reinvest + sim["spot_value"]
        portfolio_ret         = (portfolio_equity - args.initial_capital) / args.initial_capital * 100
        portfolio_cagr        = ((1 + portfolio_ret / 100) ** (1 / n_years) - 1) * 100

        # 한계 수익 (이전 비율 대비)
        marginal = 0.0
        if all_results:
            prev_ret = all_results[-1]["portfolio_ret"]
            prev_ratio = all_results[-1]["reinvest_ratio"]
            if ratio - prev_ratio > 0:
                marginal = (portfolio_ret - prev_ret) / (ratio - prev_ratio)

        # 재투자 배율 (FA 단독 대비)
        reinvest_mult = (portfolio_ret / fa["total_profit_pct"]) if fa["total_profit_pct"] > 0 else 1.0

        row = {
            "reinvest_ratio": ratio,
            "total_reinvested": sim["total_reinvested"],
            "spot_btc": sim["spot_btc"],
            "avg_cost": sim["avg_cost"],
            "spot_value": sim["spot_value"],
            "spot_return_pct": sim["spot_return_pct"],
            "mdd_2022_pct": sim["mdd_2022_pct"],
            "mdd_full_pct": sim["mdd_full_pct"],
            "portfolio_ret": round(portfolio_ret, 2),
            "portfolio_cagr": round(portfolio_cagr, 2),
            "reinvest_mult": round(reinvest_mult, 3),
            "marginal_per_10pct": round(marginal * 0.1, 2),
        }
        all_results.append(row)

        metrics = dict(row)
        metrics["fa_total_profit"] = fa["total_profit"]
        metrics["fa_cagr"] = fa_cagr
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_a",
                f"reinvest_{int(ratio*100):02d}pct",
                json.dumps({k: _safe_float(v) if isinstance(v, float) else v for k, v in metrics.items()}),
                json.dumps({"reinvest_ratio": ratio, "fa_capital_ratio": 0.50, "leverage": 2.0}),
            )

    # 결과 출력
    header = (
        f"{'재투자%':>7} | {'재투자$':>8} | {'BTC량':>9} | "
        f"{'현물$':>8} | {'현물수익%':>9} | {'2022MDD%':>8} | "
        f"{'포트수익%':>9} | {'배율':>5} | {'한계수익/10p':>11}"
    )
    print(header)
    print("-" * len(header))
    for r in all_results:
        print(
            f"{r['reinvest_ratio']:>6.0%}  | "
            f"{r['total_reinvested']:>8,.0f} | "
            f"{r['spot_btc']:>9.4f} | "
            f"{r['spot_value']:>8,.0f} | "
            f"{r['spot_return_pct']:>+8.1f}% | "
            f"{r['mdd_2022_pct']:>7.1f}% | "
            f"{r['portfolio_ret']:>+8.1f}% | "
            f"{r['reinvest_mult']:>5.2f} | "
            f"{r['marginal_per_10pct']:>+10.1f}%"
        )

    print()
    print("=== 연도별 BTC 축적 (재투자 50% 기준) ===")
    yearly = track_yearly_accumulation(trades, ohlcv, 0.50)
    hy = f"{'연도':>5} | {'FA수익$':>8} | {'재투자$':>8} | {'BTC매수':>8} | {'연말BTC가':>10} | {'누적BTC':>9} | {'현물평가$':>9} | {'현물수익%':>9}"
    print(hy)
    print("-" * len(hy))
    for y in yearly:
        print(
            f"{y['year']:>5} | "
            f"{y['fa_pnl_usd']:>8,.0f} | "
            f"{y['reinvested_usd']:>8,.0f} | "
            f"{y['btc_bought']:>8.4f} | "
            f"${y['year_end_price']:>9,.0f} | "
            f"{y['cum_btc']:>9.4f} | "
            f"{y['spot_value_usd']:>9,.0f} | "
            f"{y['spot_return_pct']:>+8.1f}%"
        )

    print()
    print(f"[DB 저장 완료] stage=stage_a, {len(all_results)}개 변형")

    await pool.close()
    return all_results


def _parse():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=START_DATE)
    p.add_argument("--end",   default=END_DATE)
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, dest="initial_capital")
    p.add_argument("--fa-capital-ratio", default=0.50, type=float, dest="fa_capital_ratio")
    p.add_argument("--leverage", default=2.0, type=float)
    p.add_argument("--reinvest-ratios", default="0.0,0.30,0.50,0.60,0.70,0.80,1.00",
                   dest="reinvest_ratios")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 중단")
        sys.exit(0)
