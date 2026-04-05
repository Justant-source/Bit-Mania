"""bt_fa_capital_ratio.py — Test 12 Stage B: FA 자본 비율 확대

FA 자본 비율을 50~95%로 변경하여 수익/리스크 분석.
베이시스 스프레드 극단값 기반 마진 안전배율 산출.

실행:
    python tests/backtest/bt_fa_capital_ratio.py
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

log = structlog.get_logger(__name__)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055
TEST_NAME       = "test_12_stage_b"
MAINT_MARGIN    = 0.005   # Bybit BTCUSDT 유지증거금률 0.5%

FA_CAPITAL_RATIOS = [0.50, 0.60, 0.70, 0.80, 0.90, 0.95]

FA_PARAMS: dict[str, Any] = {
    "exit_on_flip": True,
    "consecutive_intervals": 3,
    "min_funding_rate": 0.0001,
    "max_hold_bars": 168,
    "leverage": 2.0,
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


def estimate_basis_spread(funding: pd.DataFrame) -> dict:
    """펀딩비로부터 베이시스 스프레드 추정.

    펀딩비는 연율 기준으로 베이시스의 이동평균에 수렴.
    단순 추정: |funding_rate| × 8 (8h 정산 주기 → 1일 수익률 단위)
    """
    rates = funding["rate"].abs()

    # 8시간 단위 정산 → 일일 기준으로 환산
    basis_proxy = rates * 8  # 1일 기준 베이시스 추정

    return {
        "p99":   float(basis_proxy.quantile(0.99)),
        "p999":  float(basis_proxy.quantile(0.999)),
        "max":   float(basis_proxy.max()),
        "mean":  float(basis_proxy.mean()),
        # 2022 위기 구간 최대
        "max_2022": float(
            basis_proxy[
                (basis_proxy.index >= pd.Timestamp("2022-01-01", tz="UTC")) &
                (basis_proxy.index <= pd.Timestamp("2022-12-31", tz="UTC"))
            ].max() if not basis_proxy.empty else 0.0
        ),
    }


class FAEngineWithCapitalRatio:
    """FA 엔진 with fa_capital_ratio 파라미터."""

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

    def run(self) -> dict:
        p        = self._params
        min_rate = p.get("min_funding_rate", 0.0001)
        consec   = p.get("consecutive_intervals", 3)
        max_hold = p.get("max_hold_bars", 168)
        fa_ratio = p.get("fa_capital_ratio", 0.50)

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
        size  = self._position["size"]
        exit_ = float(bar["close"])
        fee   = exit_ * size * self._fee
        self._equity -= fee
        pnl   = self._position.get("funding_acc", 0.0) - self._position.get("fee_paid", 0.0) - fee
        self._trades.append({
            "pnl": pnl,
            "funding_acc": self._position.get("funding_acc", 0.0),
            "entry_ts": str(self._position.get("entry_ts", "")),
            "close_ts": str(bar["ts"]),
            "entry_price": self._position["entry_price"],
            "exit_price": exit_,
            "size": size,
        })
        self._position = None

    def _build_result(self) -> dict:
        total = self._equity - self._capital
        wins  = [t for t in self._trades if t["pnl"] > 0]
        loss  = [t for t in self._trades if t["pnl"] <= 0]
        gp    = sum(t["pnl"] for t in wins)
        gl    = abs(sum(t["pnl"] for t in loss))

        eq = pd.Series(self._equity_curve)
        roll = eq.cummax()
        dd   = (eq - roll) / roll
        mdd  = float(dd.min()) * 100

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
            "trades": self._trades,
        }


def calc_margin_safety(
    fa_capital_ratio: float,
    leverage: float,
    initial_capital: float,
    basis_p999: float,
) -> dict:
    """마진 안전배율 계산."""
    fa_capital       = initial_capital * fa_capital_ratio
    cash_buffer      = initial_capital * (1 - fa_capital_ratio)
    position_notional = fa_capital * leverage
    maint_margin     = position_notional * MAINT_MARGIN

    # 99.9퍼센타일 베이시스에서 미실현 손실
    unrealized_loss_p999 = position_notional * basis_p999

    # 마진 안전배율 = 현금버퍼 / (포지션명목 × 99.9p 베이시스)
    safety_mult = (cash_buffer / unrealized_loss_p999) if unrealized_loss_p999 > 0 else float("inf")

    # 5% 베이시스 극한 시나리오
    extreme_loss_5pct = position_notional * 0.05
    survives_5pct = (fa_capital + (-extreme_loss_5pct)) > maint_margin

    return {
        "fa_capital_usd":     round(fa_capital, 0),
        "cash_buffer_usd":    round(cash_buffer, 0),
        "position_notional":  round(position_notional, 0),
        "maint_margin_usd":   round(maint_margin, 0),
        "basis_p999":         round(basis_p999 * 100, 3),
        "unrealized_loss_p999": round(unrealized_loss_p999, 2),
        "safety_mult":        round(safety_mult, 2),
        "survives_5pct_basis": bool(survives_5pct),
    }


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
        print(f"[ERROR] DB 연결 실패: {exc}"); sys.exit(1)

    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 없음"); await pool.close(); sys.exit(1)

    log.info("data_loaded", ohlcv=len(ohlcv), funding=len(funding))

    # 베이시스 스프레드 극단값 분석
    basis = estimate_basis_spread(funding)
    print()
    print("=" * 80)
    print("  Test 12 Stage B: FA 자본 비율 확대")
    print("=" * 80)
    print()
    print("베이시스 스프레드 극단값 분석 (펀딩비 × 8 프록시):")
    print(f"  평균:       {basis['mean']*100:.4f}%")
    print(f"  99 퍼센타일: {basis['p99']*100:.4f}%")
    print(f"  99.9 퍼센타일: {basis['p999']*100:.4f}%")
    print(f"  최대:       {basis['max']*100:.4f}%")
    print(f"  2022 최대:  {basis['max_2022']*100:.4f}%")
    print()

    # DB 초기화
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        await conn.execute("DELETE FROM test12_results WHERE stage='stage_b'")

    all_results = []
    fa_ratios = [float(r) for r in args.fa_ratios.split(",")]

    for fa_ratio in fa_ratios:
        params = dict(FA_PARAMS)
        params["fa_capital_ratio"] = fa_ratio
        params["leverage"]         = args.leverage

        engine = FAEngineWithCapitalRatio(ohlcv, funding, params, args.initial_capital)
        result = engine.run()

        cagr   = ((1 + result["total_profit_pct"] / 100) ** (1 / n_years) - 1) * 100
        margin = calc_margin_safety(
            fa_ratio, args.leverage, args.initial_capital, basis["p999"]
        )

        # 마진 안전 등급
        if margin["safety_mult"] >= 3.0:
            safety_grade = "✅ 안전"
        elif margin["safety_mult"] >= 2.0:
            safety_grade = "⚠️ 주의"
        elif margin["safety_mult"] >= 1.0:
            safety_grade = "❌ 위험"
        else:
            safety_grade = "🚫 청산가능"

        row = {
            "fa_capital_ratio": fa_ratio,
            "total_profit_pct": result["total_profit_pct"],
            "cagr": round(cagr, 2),
            "sharpe_ratio": result["sharpe_ratio"],
            "max_drawdown_pct": result["max_drawdown_pct"],
            "total_trades": result["total_trades"],
            "cash_buffer_usd": margin["cash_buffer_usd"],
            "position_notional": margin["position_notional"],
            "safety_mult": margin["safety_mult"],
            "safety_grade": safety_grade,
            "survives_5pct": margin["survives_5pct_basis"],
            "basis_p999_pct": margin["basis_p999"],
        }
        all_results.append(row)

        metrics = {**row, **{k: _safe_float(v) if isinstance(v, float) else v
                              for k, v in margin.items()}}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO test12_results(stage,variant,metrics,params) "
                "VALUES($1,$2,$3::jsonb,$4::jsonb)",
                "stage_b",
                f"fa_ratio_{int(fa_ratio*100):02d}pct",
                json.dumps({k: _safe_float(v) if isinstance(v, float) else v
                            for k, v in metrics.items()}),
                json.dumps({"fa_capital_ratio": fa_ratio, "leverage": args.leverage,
                            "reinvest_ratio": args.reinvest_ratio}),
            )
        log.info("done", fa_ratio=fa_ratio, cagr=cagr, safety=margin["safety_mult"])

    # 출력
    header = (
        f"{'FA비율':>7} | {'연수익%':>7} | {'Sharpe':>7} | {'MDD%':>6} | "
        f"{'현금버퍼$':>8} | {'포지션명목$':>11} | {'마진안전배율':>10} | {'판정':>12}"
    )
    print(header)
    print("-" * len(header))
    for r in all_results:
        print(
            f"{r['fa_capital_ratio']:>6.0%}  | "
            f"{r['cagr']:>+6.2f}% | "
            f"{r['sharpe_ratio']:>7.3f} | "
            f"{r['max_drawdown_pct']:>5.2f}% | "
            f"${r['cash_buffer_usd']:>7,.0f} | "
            f"${r['position_notional']:>10,.0f} | "
            f"{r['safety_mult']:>10.2f}x | "
            f"{r['safety_grade']:>12}"
        )

    print()
    print("판정 기준: 마진안전배율 ≥3배=안전, 2~3배=주의, 1~2배=위험, <1배=청산가능")
    print(f"\n[DB 저장 완료] stage=stage_b, {len(all_results)}개 변형")

    await pool.close()
    return all_results


def _parse():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=START_DATE)
    p.add_argument("--end",   default=END_DATE)
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, dest="initial_capital")
    p.add_argument("--fa-ratios", default="0.50,0.60,0.70,0.80,0.90,0.95", dest="fa_ratios")
    p.add_argument("--leverage", default=2.0, type=float)
    p.add_argument("--reinvest-ratio", default=0.30, type=float, dest="reinvest_ratio")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 중단"); sys.exit(0)
