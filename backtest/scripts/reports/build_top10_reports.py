#!/usr/bin/env python3
"""
Build Top-10 Strategy Reports

Generates per-period MD files in backtest/results/top10/ with:
- Top 10 strategies ranked by total return multiplier
- Full performance metrics
- Monthly breakdown
- All individual trades

Periods:
  Post-2021 yearly:  2021, 2022, 2023, 2024, 2025
  Pre-2021 regimes:  pre21_bear, pre21_bull, pre21_covid

Usage:
  python3 backtest/scripts/reports/build_top10_reports.py
"""

import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BT_ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_7S = BT_ROOT / "results" / "7-strategies"
RESULTS_PRE21 = BT_ROOT / "results" / "pre2021_backfill"
ADJUSTED_JSON = BT_ROOT / "results" / "adjusted_costs_pre2021" / "all_adjusted_results_pre21.json"
ADJUSTED_POST21_JSON = BT_ROOT / "results" / "adjusted_costs_7strategies" / "all_adjusted_results_7s.json"
FUNDING_PARQUET = BT_ROOT / "data" / "funding" / "BTCUSDT_8h.parquet"
OUT_DIR = BT_ROOT / "results" / "top10"

# ── Pre-2021 period metadata ───────────────────────────────────────────────────
PRE21_PERIODS = {
    "pre21_bear": {
        "label": "Pre-2021 Bear Market",
        "range": "2017-12-17 ~ 2018-12-15",
        "context": "BTC crashed from ATH ~$19,800 to low ~$3,200. Total drawdown -83.7%.",
        "years": 0.99,
    },
    "pre21_bull": {
        "label": "Pre-2021 Bull Market",
        "range": "2020-05-01 ~ 2020-12-31",
        "context": "Bitcoin halving aftermath bull run, BTC from ~$8,700 to ~$29,000. +233% gain.",
        "years": 0.67,
    },
    "pre21_covid": {
        "label": "Pre-2021 COVID Crash",
        "range": "2020-03-01 ~ 2020-04-30",
        "context": "COVID-19 pandemic crash. BTC dropped from ~$9,000 to $4,700 then recovered to ~$9,400.",
        "years": 0.17,
    },
}

# ── Post-2021 year metadata ────────────────────────────────────────────────────
YEAR_CONTEXTS = {
    2021: "BTC from $29k to ATH $69k (Nov), then reversal to $47k year-end. Crypto bull peak.",
    2022: "Bear market: BTC from $47k to low $15.7k (-76%). FTX collapse, LUNA crash.",
    2023: "Recovery year: BTC from $16.6k to $42k (+152%). ETF anticipation narrative.",
    2024: "Spot ETF approved Jan 11, BTC new ATH $73.8k (Mar). Halving Apr. Strong bull.",
    2025: "Continued bull: BTC above $80k region. Data through 2026-04-30.",
}

STARTING_BALANCE = 10_000.0


# ── Utilities ─────────────────────────────────────────────────────────────────

def ts_to_dt(ms: float) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def fmt_dt(ms: float) -> str:
    return ts_to_dt(ms).strftime("%Y-%m-%d %H:%M")


def year_of_ts(ms: float) -> int:
    return ts_to_dt(ms).year


def month_of_ts(ms: float) -> str:
    return ts_to_dt(ms).strftime("%Y-%m")


def holding_hours(open_ms: float, close_ms: float) -> float:
    return (close_ms - open_ms) / 3_600_000.0


def pct(val: float) -> str:
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"


def multiplier_str(mult: float) -> str:
    if mult >= 10000:
        return f"{mult:,.0f}x"
    if mult >= 1000:
        return f"{mult:.0f}x"
    if mult >= 10:
        return f"{mult:.1f}x"
    return f"{mult:.2f}x"


def extract_leverage(variant: str) -> int:
    m = re.search(r'_x(\d+)$', variant)
    return int(m.group(1)) if m else 1


def load_trades(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        reader = csv.DictReader(f)
        return [
            {
                "opened_at": float(r["opened_at"]),
                "closed_at": float(r["closed_at"]),
                "side": r["side"],
                "entry_price": float(r["entry_price"]),
                "exit_price": float(r["exit_price"]),
                "qty": float(r["qty"]),
                "pnl": float(r["pnl"]),
                "fee": float(r["fee"]),
            }
            for r in reader
        ]


def load_stats(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


# ── Post-2021 yearly slice helpers ────────────────────────────────────────────

def slice_trades_by_year(trades: list[dict], year: int) -> list[dict]:
    """Trades where opened_at falls in the given calendar year."""
    return [t for t in trades if year_of_ts(t["opened_at"]) == year]


def simulate_equity(trades: list[dict], starting: float = STARTING_BALANCE) -> tuple[float, list[dict]]:
    """
    Accumulate pnl (already net of fees) from a sorted trade list.
    Returns (finishing_balance, trades_with_equity).
    """
    equity = starting
    enriched = []
    for t in sorted(trades, key=lambda x: x["opened_at"]):
        equity += t["pnl"]
        enriched.append({**t, "equity_after": equity})
    return equity, enriched


def monthly_breakdown(trades: list[dict], starting: float) -> list[dict]:
    """Group trades by month of opened_at, return monthly stats."""
    by_month: dict[str, list] = {}
    for t in sorted(trades, key=lambda x: x["opened_at"]):
        m = month_of_ts(t["opened_at"])
        by_month.setdefault(m, []).append(t)

    rows = []
    equity = starting
    for month in sorted(by_month):
        month_trades = by_month[month]
        pnl_sum = sum(t["pnl"] for t in month_trades)
        fee_sum = sum(t["fee"] for t in month_trades)
        start_eq = equity
        equity += pnl_sum
        ret_pct = (equity - start_eq) / start_eq * 100 if start_eq else 0
        rows.append({
            "month": month,
            "trades": len(month_trades),
            "pnl": pnl_sum,
            "fee": fee_sum,
            "start_eq": start_eq,
            "end_eq": equity,
            "ret_pct": ret_pct,
        })
    return rows


def compute_win_rate(trades: list[dict]) -> float:
    if not trades:
        return 0.0
    wins = sum(1 for t in trades if t["pnl"] > 0)
    return wins / len(trades) * 100


def compute_max_drawdown(trades: list[dict], starting: float) -> float:
    """Max drawdown % from peak equity."""
    equity = starting
    peak = equity
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x["opened_at"]):
        equity += t["pnl"]
        if equity > peak:
            peak = equity
        dd = (equity - peak) / peak * 100
        if dd < max_dd:
            max_dd = dd
    return max_dd


# ── Collect all post-2021 combos ──────────────────────────────────────────────

def collect_post21_combos() -> list[dict]:
    """Return list of {strat, tf, variant, trades, stats_path} for all 7-strategies combos."""
    combos = []
    for strat_dir in sorted(RESULTS_7S.iterdir()):
        strat = strat_dir.name
        if strat in ("archive", "buy_and_hold", "rolling_window") or not strat_dir.is_dir():
            continue
        for tf_dir in sorted(strat_dir.iterdir()):
            if not tf_dir.is_dir() or tf_dir.name == "walk_forward":
                continue
            tf = tf_dir.name
            for variant_dir in sorted(tf_dir.iterdir()):
                if not variant_dir.is_dir():
                    continue
                variant = variant_dir.name
                trades_path = variant_dir / "trades.csv"
                stats_path = variant_dir / "stats.json"
                if not trades_path.exists() or not stats_path.exists():
                    continue
                trades = load_trades(trades_path)
                combos.append({
                    "strat": strat,
                    "tf": tf,
                    "variant": variant,
                    "label": f"{strat}/{tf}/{variant}",
                    "trades": trades,
                    "stats_path": stats_path,
                })
    return combos


# ── Collect all pre-2021 combos ───────────────────────────────────────────────

def collect_pre21_combos(period: str) -> list[dict]:
    """Return list of {strat, tf, variant, period, trades, stats} for a period."""
    combos = []
    for strat_dir in sorted(RESULTS_PRE21.iterdir()):
        strat = strat_dir.name
        if not strat_dir.is_dir():
            continue
        for tf_dir in sorted(strat_dir.iterdir()):
            if not tf_dir.is_dir():
                continue
            tf = tf_dir.name
            for variant_dir in sorted(tf_dir.iterdir()):
                if not variant_dir.is_dir():
                    continue
                variant = variant_dir.name
                period_dir = variant_dir / period
                if not period_dir.exists():
                    continue
                trades_path = period_dir / "trades.csv"
                stats_path = period_dir / "stats.json"
                if not stats_path.exists():
                    continue
                trades = load_trades(trades_path) if trades_path.exists() else []
                stats = load_stats(stats_path)
                if stats is None:
                    continue
                combos.append({
                    "strat": strat,
                    "tf": tf,
                    "variant": variant,
                    "period": period,
                    "label": f"{strat}/{tf}/{variant}",
                    "trades": trades,
                    "stats": stats,
                })
    return combos


# ── Load adjusted costs lookups ───────────────────────────────────────────────

def load_adjusted_lookup() -> dict:
    """Returns {(strat, tf, variant, period): adj_data} from all_adjusted_results_pre21.json"""
    if not ADJUSTED_JSON.exists():
        return {}
    with open(ADJUSTED_JSON) as f:
        results = json.load(f)
    lookup = {}
    for entry in results:
        strat = entry.get("strat", "")
        tf = entry.get("tf", "")
        variant = entry.get("variant", "")
        for period, pdata in entry.get("periods", {}).items():
            lookup[(strat, tf, variant, period)] = pdata
    return lookup


def load_post21_adjusted_lookup() -> dict:
    """Returns {(strat, tf, variant, period): adj_data} from all_adjusted_results_7s.json"""
    if not ADJUSTED_POST21_JSON.exists():
        return {}
    with open(ADJUSTED_POST21_JSON) as f:
        results = json.load(f)
    lookup = {}
    for entry in results:
        strat = entry.get("strat", "")
        tf = entry.get("tf", "")
        variant = entry.get("variant", "")
        for period, pdata in entry.get("periods", {}).items():
            lookup[(strat, tf, variant, period)] = pdata
    return lookup


def load_funding_df():
    """Load BTCUSDT 8h funding rates into a sorted DataFrame."""
    if not FUNDING_PARQUET.exists():
        print(f"[warn] Funding parquet not found: {FUNDING_PARQUET}", file=sys.stderr)
        return None
    df = pd.read_parquet(FUNDING_PARQUET)
    return df.sort_values("timestamp").reset_index(drop=True)


def variant_funding_sign(variant: str) -> float:
    base = re.sub(r'_x\d+$', '', variant)
    if base == "long_only":
        return +1.0
    if base == "short_only":
        return -1.0
    return 0.0


def trade_funding_cost(trade: dict, funding_df, f_sign: float, lev_mult: int) -> float:
    """
    Per-trade funding cost (USDT). Positive = cost (long pays), negative = income (short receives).
    bidirectional (f_sign=0) always returns 0.
    """
    if funding_df is None or f_sign == 0:
        return 0.0
    open_ms = trade["opened_at"]
    close_ms = trade["closed_at"]
    notional = trade["entry_price"] * trade["qty"]
    ts = funding_df["timestamp"].values
    rates = funding_df["funding_rate"].values
    mask = (ts >= open_ms) & (ts < close_ms)
    if not np.any(mask):
        return 0.0
    return float(rates[mask].sum()) * notional * f_sign * lev_mult


# ── Markdown rendering helpers ────────────────────────────────────────────────

def trades_table_md(trades: list[dict], variant: str = "bidirectional", funding_df=None) -> str:
    if not trades:
        return "_No trades in this period._\n"
    f_sign = variant_funding_sign(variant)
    lev_mult = extract_leverage(variant)
    lines = [
        "| # | Opened (UTC) | Closed (UTC) | Side | Entry | Exit | Qty (BTC) | Hold (h) | Fee (USDT) | Funding (USDT) | PnL (USDT) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, t in enumerate(sorted(trades, key=lambda x: x["opened_at"]), 1):
        hold = holding_hours(t["opened_at"], t["closed_at"])
        funding = trade_funding_cost(t, funding_df, f_sign, lev_mult)
        fund_str = ('+' if funding > 0 else '') + f"{funding:.2f}"
        pnl_sign = "+" if t["pnl"] >= 0 else ""
        lines.append(
            f"| {i} "
            f"| {fmt_dt(t['opened_at'])} "
            f"| {fmt_dt(t['closed_at'])} "
            f"| {t['side'].upper()} "
            f"| ${t['entry_price']:,.1f} "
            f"| ${t['exit_price']:,.1f} "
            f"| {t['qty']:.4f} "
            f"| {hold:.1f} "
            f"| {t['fee']:.2f} "
            f"| {fund_str} "
            f"| {pnl_sign}{t['pnl']:.2f} |"
        )
    return "\n".join(lines) + "\n"


def monthly_table_md(months: list[dict]) -> str:
    if not months:
        return "_No monthly data._\n"
    lines = [
        "| Month | Trades | PnL (USDT) | Fee (USDT) | Start Equity | End Equity | Return |",
        "|-------|--------|-----------|-----------|-------------|-----------|--------|",
    ]
    for m in months:
        sign = "+" if m["ret_pct"] >= 0 else ""
        lines.append(
            f"| {m['month']} "
            f"| {m['trades']} "
            f"| {'+' if m['pnl']>=0 else ''}{m['pnl']:.2f} "
            f"| {m['fee']:.2f} "
            f"| ${m['start_eq']:,.2f} "
            f"| ${m['end_eq']:,.2f} "
            f"| {sign}{m['ret_pct']:.2f}% |"
        )
    return "\n".join(lines) + "\n"


# ── Build post-2021 yearly report ─────────────────────────────────────────────

def build_yearly_report(year: int, combos: list[dict],
                         post21_lookup: dict, funding_df) -> str:
    """Build MD content for a post-2021 yearly top10 report."""
    print(f"  Processing year {year}...", file=sys.stderr)

    # Compute per-combo return for this year
    ranked = []
    for c in combos:
        year_trades = slice_trades_by_year(c["trades"], year)
        if not year_trades:
            continue
        finishing, enriched = simulate_equity(year_trades)
        total_return = (finishing - STARTING_BALANCE) / STARTING_BALANCE * 100
        mult = finishing / STARTING_BALANCE
        win_rate = compute_win_rate(year_trades)
        max_dd = compute_max_drawdown(year_trades, STARTING_BALANCE)
        total_fee = sum(t["fee"] for t in year_trades)

        # Funding-adjusted costs
        adj = post21_lookup.get((c["strat"], c["tf"], c["variant"], str(year)), {})
        fee_cost = adj.get("fee_cost_annual_pct", 0.0)
        fund_cost = adj.get("funding_cost_annual_pct", 0.0)
        funding_coverage = adj.get("funding_coverage", "fee_only")
        # Adjusted finishing: subtract cost adjustments from starting-normalised return
        # fee_cost is already implicit in Jesse trades (maker delta), fund_cost is additive
        adj_return_pct = total_return - fee_cost - fund_cost
        adj_finishing = STARTING_BALANCE * (1 + adj_return_pct / 100)
        adj_mult = adj_finishing / STARTING_BALANCE

        ranked.append({
            **c,
            "year_trades": year_trades,
            "finishing": finishing,
            "total_return": total_return,
            "mult": mult,
            "adj_return_pct": adj_return_pct,
            "adj_finishing": adj_finishing,
            "adj_mult": adj_mult,
            "win_rate": win_rate,
            "max_dd": max_dd,
            "total_fee": total_fee,
            "fee_cost": fee_cost,
            "fund_cost": fund_cost,
            "funding_coverage": funding_coverage,
        })

    # Rank by adjusted return (funding reflected)
    ranked.sort(key=lambda x: x["adj_mult"], reverse=True)
    top10 = ranked[:10]

    ctx = YEAR_CONTEXTS.get(year, "")
    lines = [
        f"# {year}년 Top 10 전략 성과 보고서",
        "",
        f"**생성일**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}  ",
        f"**분석 기간**: {year}-01-01 ~ {year}-12-31  ",
        f"**시작 자본**: $10,000 (연도별 정규화)  ",
        f"**시장 맥락**: {ctx}",
        "",
        "> **비용 모델**: 수수료 0.02% (메이커, Jesse 시뮬레이션 내 반영) + 추가 수수료 델타(maker→taker 0.035%/side) + 펀딩비 (Bybit 실데이터).",
        "> 펀딩비 = 롱: 비용(양수), 쇼트: 수익(음수), 양방향: 0 (long/short 상쇄 가정).",
        "> **순위는 펀딩비 반영 후 조정 수익률 기준**.",
        "",
        "---",
        "",
        "## 전략 순위 요약 (조정 수익률 기준)",
        "",
        "| 순위 | 전략 | 거래 수 | 원본 수익률 | 펀딩비 | 조정 수익률 | MDD | 승률 |",
        "|-----|------|--------|----------|------|----------|-----|-----|",
    ]

    for rank, c in enumerate(top10, 1):
        orig_sign = "+" if c["total_return"] >= 0 else ""
        adj_sign = "+" if c["adj_return_pct"] >= 0 else ""
        fund_sign = "-" if c["fund_cost"] >= 0 else "+"
        lines.append(
            f"| {rank} "
            f"| `{c['label']}` "
            f"| {len(c['year_trades'])} "
            f"| {orig_sign}{c['total_return']:.1f}% (${c['finishing']:,.0f}) "
            f"| {fund_sign}{abs(c['fund_cost']):.2f}%/yr "
            f"| **{adj_sign}{c['adj_return_pct']:.1f}%** (${c['adj_finishing']:,.0f}) "
            f"| {c['max_dd']:.1f}% "
            f"| {c['win_rate']:.1f}% |"
        )

    lines += [
        "",
        f"전체 분석 대상: **{len(ranked)}개 전략/변형** (해당 연도 거래 있음)  ",
        f"최고 수익(조정 후): **{multiplier_str(top10[0]['adj_mult'])}** ({top10[0]['label']})  ",
        f"최저 수익(Top10 기준): **{multiplier_str(top10[-1]['adj_mult'])}** ({top10[-1]['label']})",
        "",
        "---",
        "",
    ]

    # Detailed section per strategy
    for rank, c in enumerate(top10, 1):
        label = c["label"]
        lev = extract_leverage(c["variant"])
        lev_note = f" (레버리지 {lev}x 포지션)" if lev > 1 else ""
        orig_sign = "+" if c["total_return"] >= 0 else ""
        adj_sign = "+" if c["adj_return_pct"] >= 0 else ""

        lines += [
            f"## #{rank} {label}{lev_note}",
            "",
            f"### 성과 요약",
            "",
            f"| 항목 | 값 |",
            f"|-----|---|",
            f"| 전략 | `{c['strat']}` |",
            f"| 타임프레임 | `{c['tf']}` |",
            f"| 변형 | `{c['variant']}` |",
            f"| 레버리지 | {lev}x |",
            f"| 시작 자본 | $10,000 |",
            f"| 최종 자산 (원본) | ${c['finishing']:,.2f} |",
            f"| 원본 수익률 | {orig_sign}{c['total_return']:.2f}% |",
            f"| 추가 수수료 비용 | -{c['fee_cost']:.2f}%/yr |",
            f"| 펀딩비 비용 | -{c['fund_cost']:.2f}%/yr |",
            f"| 펀딩비 커버리지 | {c['funding_coverage']} |",
            f"| 조정 수익률 | **{adj_sign}{c['adj_return_pct']:.2f}%** |",
            f"| 조정 최종 자산 | **${c['adj_finishing']:,.2f}** |",
            f"| 거래 수 | {len(c['year_trades'])} |",
            f"| 승률 | {c['win_rate']:.2f}% |",
            f"| 최대 낙폭(MDD) | {c['max_dd']:.2f}% |",
            f"| 총 수수료 (Jesse) | ${c['total_fee']:.2f} |",
            "",
        ]

        # Longs vs Shorts breakdown
        longs = [t for t in c["year_trades"] if t["side"] == "long"]
        shorts = [t for t in c["year_trades"] if t["side"] == "short"]
        long_pnl = sum(t["pnl"] for t in longs)
        short_pnl = sum(t["pnl"] for t in shorts)
        lines += [
            f"**방향별 분류**: 롱 {len(longs)}건 (PnL {'+' if long_pnl>=0 else ''}{long_pnl:.2f} USDT) / "
            f"숏 {len(shorts)}건 (PnL {'+' if short_pnl>=0 else ''}{short_pnl:.2f} USDT)",
            "",
        ]

        # Monthly breakdown
        months = monthly_breakdown(c["year_trades"], STARTING_BALANCE)
        lines += [
            f"### 월별 손익",
            "",
            monthly_table_md(months),
        ]

        # Individual trades
        lines += [
            f"### 개별 거래 내역 (총 {len(c['year_trades'])}건)",
            "> Hold(h): 보유시간 | Fee: Jesse 수수료(0.02%) | Funding: 펀딩비(양수=비용, 음수=수익) | PnL: Jesse 순손익",
            "",
            trades_table_md(c["year_trades"], c["variant"], funding_df),
            "---",
            "",
        ]

    # Bottom performers for context
    if len(ranked) > 10:
        bottom = ranked[-5:] if len(ranked) >= 5 else ranked[10:]
        lines += [
            "## 참고: 하위 성과 전략 (맥락 파악용)",
            "",
            "| 순위 | 전략 | 거래 수 | 원본 수익률 | 조정 수익률 | MDD |",
            "|-----|------|--------|----------|----------|-----|",
        ]
        for rank_b, c in enumerate(reversed(bottom), 1):
            overall_rank = len(ranked) - len(bottom) + rank_b
            orig_sign = "+" if c["total_return"] >= 0 else ""
            adj_sign = "+" if c["adj_return_pct"] >= 0 else ""
            lines.append(
                f"| {overall_rank}/{len(ranked)} "
                f"| `{c['label']}` "
                f"| {len(c['year_trades'])} "
                f"| {orig_sign}{c['total_return']:.1f}% "
                f"| {adj_sign}{c['adj_return_pct']:.1f}% "
                f"| {c['max_dd']:.1f}% |"
            )
        lines.append("")

    return "\n".join(lines)


# ── Build pre-2021 regime report ──────────────────────────────────────────────

def build_pre21_report(period: str, adjusted_lookup: dict, funding_df=None) -> str:
    """Build MD content for a pre-2021 regime top10 report."""
    meta = PRE21_PERIODS[period]
    print(f"  Processing {period} ({meta['label']})...", file=sys.stderr)

    combos = collect_pre21_combos(period)

    ranked = []
    for c in combos:
        stats = c["stats"]
        raw = stats.get("raw_metrics", {})
        starting = raw.get("starting_balance", STARTING_BALANCE)
        finishing = raw.get("finishing_balance", starting)
        total_return = (finishing - starting) / starting * 100 if starting else 0
        mult = finishing / starting if starting else 1.0

        # Adjusted metrics
        adj_key = (c["strat"], c["tf"], c["variant"], period)
        adj_data = adjusted_lookup.get(adj_key, {})
        adj_cagr = adj_data.get("adj_cagr", stats.get("cagr_pct", 0))
        original_cagr = adj_data.get("original_cagr", stats.get("cagr_pct", 0))
        fee_cost = adj_data.get("fee_cost_annual_pct", 0)
        fund_cost = adj_data.get("funding_cost_annual_pct", 0)
        funding_coverage = adj_data.get("funding_coverage", "fee_only")

        win_rate = raw.get("win_rate", 0) * 100
        max_dd = stats.get("max_drawdown_pct", 0)
        total_fee = raw.get("fee", 0)

        ranked.append({
            **c,
            "starting": starting,
            "finishing": finishing,
            "total_return": total_return,
            "mult": mult,
            "win_rate": win_rate,
            "max_dd": max_dd,
            "total_fee": total_fee,
            "adj_cagr": adj_cagr,
            "original_cagr": original_cagr,
            "fee_cost": fee_cost,
            "fund_cost": fund_cost,
            "funding_coverage": funding_coverage,
            "sharpe": stats.get("sharpe_ratio", 0),
            "total_trades": stats.get("total_trades", 0),
        })

    ranked.sort(key=lambda x: x["mult"], reverse=True)
    top10 = ranked[:10]

    # Funding coverage note by period
    coverage_notes = {
        "pre21_bear":     "❌ 펀딩 데이터 없음 (Binance perp 2019-09 출시 전) — fee_only",
        "pre21_range":    "❌ 펀딩 데이터 없음 (Binance perp 2019-09 출시 전) — fee_only",
        "pre21_recovery": "△ 부분 커버 (2019-09 이후 실데이터, 이전 fee_only) — mixed",
        "pre21_covid":    "✓ 실데이터 (Bybit + Binance vision) — mixed",
        "pre21_bull":     "✓ 실데이터 (Bybit live) — bybit_live",
        "pre21_full":     "△ 부분 커버 (2019-09 이후만 실데이터) — mixed",
    }
    cov_note = coverage_notes.get(period, "")

    lines = [
        f"# Pre-2021 {meta['label']} Top 10 전략 보고서",
        "",
        f"**생성일**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}  ",
        f"**분석 기간**: {meta['range']}  ",
        f"**기간(연)**: {meta['years']:.2f}년  ",
        f"**시작 자본**: $10,000  ",
        f"**시장 맥락**: {meta['context']}",
        "",
        f"> **펀딩비 커버리지**: {cov_note}",
        "> 조정 CAGR = 원본 CAGR − 수수료 델타 − 펀딩비 (레버리지 스케일 적용: ×lev_mult).",
        "> 순위는 Jesse 원본 총수익률(거래 내 수수료 반영) 기준.",
        "",
        "---",
        "",
        "## 전략 순위 요약 (총수익률 기준)",
        "",
        "| 순위 | 전략 | 거래 수 | 시작 자산 | 최종 자산 | 수익 배수 | 원본 CAGR | 조정 CAGR | MDD | 승률 |",
        "|-----|------|--------|---------|---------|---------|--------|--------|-----|-----|",
    ]

    for rank, c in enumerate(top10, 1):
        sign = "+" if c["adj_cagr"] >= 0 else ""
        orig_sign = "+" if c["original_cagr"] >= 0 else ""
        lines.append(
            f"| {rank} "
            f"| `{c['label']}` "
            f"| {c['total_trades']} "
            f"| ${c['starting']:,.0f} "
            f"| ${c['finishing']:,.2f} "
            f"| **{multiplier_str(c['mult'])}** "
            f"| {orig_sign}{c['original_cagr']:.2f}%/yr "
            f"| {sign}{c['adj_cagr']:.2f}%/yr "
            f"| {c['max_dd']:.2f}% "
            f"| {c['win_rate']:.1f}% |"
        )

    lines += [
        "",
        f"전체 분석 대상: **{len(ranked)}개 전략/변형**  ",
        f"최고 수익: **{multiplier_str(top10[0]['mult'])}** ({top10[0]['label']})  ",
        f"최저 수익(Top10 기준): **{multiplier_str(top10[-1]['mult'])}** ({top10[-1]['label']})",
        "",
        "---",
        "",
    ]

    # Detailed section per strategy
    for rank, c in enumerate(top10, 1):
        label = c["label"]
        lev = extract_leverage(c["variant"])
        lev_note = f" (레버리지 {lev}x)" if lev > 1 else ""
        sign_r = "+" if c["total_return"] >= 0 else ""
        sign_a = "+" if c["adj_cagr"] >= 0 else ""
        sign_o = "+" if c["original_cagr"] >= 0 else ""

        lines += [
            f"## #{rank} {label}{lev_note}",
            "",
            f"### 성과 요약",
            "",
            f"| 항목 | 값 |",
            f"|-----|---|",
            f"| 전략 | `{c['strat']}` |",
            f"| 타임프레임 | `{c['tf']}` |",
            f"| 변형 | `{c['variant']}` |",
            f"| 레버리지 | {lev}x |",
            f"| 분석 기간 | {meta['range']} ({meta['years']:.2f}년) |",
            f"| 시작 자본 | ${c['starting']:,.0f} |",
            f"| 최종 자산 | ${c['finishing']:,.2f} |",
            f"| 총수익 배수 | **{multiplier_str(c['mult'])}** |",
            f"| 총수익률 | {sign_r}{c['total_return']:.2f}% |",
            f"| 원본 CAGR | {sign_o}{c['original_cagr']:.2f}%/yr |",
            f"| 조정 CAGR (비용 후) | **{sign_a}{c['adj_cagr']:.2f}%/yr** |",
            f"| 비용: 수수료 | -{c['fee_cost']:.2f}%/yr |",
            f"| 비용: 펀딩비 | -{c['fund_cost']:.2f}%/yr |",
            f"| 펀딩비 데이터 | {c['funding_coverage']} |",
            f"| 거래 수 | {c['total_trades']} |",
            f"| 승률 | {c['win_rate']:.2f}% |",
            f"| 샤프 비율 | {c['sharpe']:.3f} |",
            f"| 최대 낙폭(MDD) | {c['max_dd']:.2f}% |",
            f"| 총 수수료 | ${c['total_fee']:.2f} |",
            "",
        ]

        # Longs vs Shorts breakdown
        if c["trades"]:
            longs = [t for t in c["trades"] if t["side"] == "long"]
            shorts = [t for t in c["trades"] if t["side"] == "short"]
            long_pnl = sum(t["pnl"] for t in longs)
            short_pnl = sum(t["pnl"] for t in shorts)
            lines += [
                f"**방향별**: 롱 {len(longs)}건 (PnL {'+' if long_pnl>=0 else ''}{long_pnl:.2f} USDT) / "
                f"숏 {len(shorts)}건 (PnL {'+' if short_pnl>=0 else ''}{short_pnl:.2f} USDT)",
                "",
            ]

        # Monthly breakdown
        months = monthly_breakdown(c["trades"], c["starting"])
        lines += [
            f"### 월별 손익",
            "",
            monthly_table_md(months),
        ]

        # Individual trades
        lines += [
            f"### 개별 거래 내역 (총 {len(c['trades'])}건)",
            "> Hold(h): 보유시간 | Fee: Jesse 수수료(0.02%) | Funding: 펀딩비(양수=비용, 음수=수익) | PnL: Jesse 순손익",
            "",
            trades_table_md(c["trades"], c["variant"], funding_df),
            "---",
            "",
        ]

    # Bottom performers
    if len(ranked) > 10:
        bottom = ranked[-5:] if len(ranked) >= 5 else ranked[10:]
        lines += [
            "## 참고: 하위 성과 전략 (맥락 파악용)",
            "",
            "| 순위 | 전략 | 최종 자산 | 조정 CAGR | MDD |",
            "|-----|------|---------|--------|-----|",
        ]
        for rank_b, c in enumerate(reversed(bottom), 1):
            overall_rank = len(ranked) - len(bottom) + rank_b
            sign = "+" if c["adj_cagr"] >= 0 else ""
            lines.append(
                f"| {overall_rank}/{len(ranked)} "
                f"| `{c['label']}` "
                f"| ${c['finishing']:,.2f} "
                f"| {sign}{c['adj_cagr']:.2f}%/yr "
                f"| {c['max_dd']:.2f}% |"
            )
        lines.append("")

    return "\n".join(lines)


# ── Build cross-period analysis context ───────────────────────────────────────

def build_index_md(generated_files: list[str]) -> str:
    lines = [
        "# Top 10 전략 보고서 인덱스",
        "",
        f"**생성일**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## 파일 목록",
        "",
        "| 파일 | 기간 | 설명 |",
        "|-----|------|------|",
        "| [2021_top10.md](2021_top10.md) | 2021년 | BTC 최고가 달성 후 반전 |",
        "| [2022_top10.md](2022_top10.md) | 2022년 | FTX 붕괴, 극심한 하락장 |",
        "| [2023_top10.md](2023_top10.md) | 2023년 | 회복 장세, ETF 기대감 |",
        "| [2024_top10.md](2024_top10.md) | 2024년 | 현물 ETF 승인, 신 ATH |",
        "| [2025_top10.md](2025_top10.md) | 2025년 | 강세 지속 |",
        "| [pre21_bear_top10.md](pre21_bear_top10.md) | 2017-12~2018-12 | 첫 대하락장 |",
        "| [pre21_bull_top10.md](pre21_bull_top10.md) | 2020-05~2020-12 | 하반기 강세 |",
        "| [pre21_covid_top10.md](pre21_covid_top10.md) | 2020-03~04 | 코로나 급락/반등 |",
        "",
        "## 분석 가이드 (Opus 4.7 활용)",
        "",
        "이 데이터를 바탕으로 Opus 4.7에게 다음 질문들을 던질 수 있습니다:",
        "",
        "### 단일 전략 선택",
        "- 어느 전략이 **강세/약세/변동성 구간** 모두에서 강건한가?",
        "- MDD 대비 수익률(Calmar)이 가장 우수한 전략은?",
        "- 각 구간에서 일관되게 상위권을 유지하는 전략은?",
        "",
        "### 전략 복합화",
        "- 강세에 강한 전략 + 약세에 강한 전략의 **상관관계**는?",
        "- 트레이드 타이밍이 겹치지 않아 자본 효율이 높은 조합은?",
        "- 레버리지 변형 사용 시 리스크/수익 최적점은?",
        "",
        "### 리스크 관리",
        "- 각 전략의 최대 연속 손실 기간(underwater period)은?",
        "- 월별 손익의 변동성 패턴 — 계절성이 있는가?",
        "- x2/x3 레버리지 사용 시 실제 청산 위험 구간은?",
        "",
        "## 데이터 주의사항",
        "",
        "- **비용 모델**: 수수료 0.02% (메이커, Jesse 내 반영) + 추가 델타 0.035%/side + **펀딩비 실데이터 적용**",
        "- **펀딩비 커버리지**: Pre-2021 bear/range = 데이터 없음(fee_only), recovery/covid/bull = 부분/전체 적용",
        "- **펀딩비 sign**: 롱 전용(+1, 비용), 숏 전용(-1, 수익), 양방향(0, long/short 상쇄 가정)",
        "- **레버리지 스케일**: 펀딩비 × lev_mult (x2→2배, x3→3배)",
        "- **거래 테이블 컬럼**: 보유시간 | 수수료(Jesse) | 펀딩비(post-hoc) | 손익(Jesse 순)",
        "- **슬리피지**: 미반영 (백테스트 한계)",
        "- **연도별 정규화**: 각 연도 $10,000 시작 (연속 운용 아님 — 비교용 정규화)",
        "- **레버리지**: Jesse research mode = 청산 없음 (실제 거래 시 마진 관리 필수)",
        "- **Post-2021 순위**: 펀딩비 반영 후 조정 수익률 기준",
        "",
    ]
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUT_DIR}", file=sys.stderr)

    # Load adjusted cost lookups
    adjusted_lookup = load_adjusted_lookup()
    print(f"Loaded {len(adjusted_lookup)} pre-2021 adjusted cost entries", file=sys.stderr)

    post21_lookup = load_post21_adjusted_lookup()
    print(f"Loaded {len(post21_lookup)} post-2021 adjusted cost entries", file=sys.stderr)

    # Load funding data for per-trade funding column
    funding_df = load_funding_df()
    if funding_df is not None:
        print(f"Loaded funding df: {len(funding_df)} rows", file=sys.stderr)

    # Collect post-2021 combos once (reuse across years)
    print("Collecting post-2021 combos...", file=sys.stderr)
    post21_combos = collect_post21_combos()
    print(f"  {len(post21_combos)} combos", file=sys.stderr)

    generated = []

    # Post-2021 yearly reports
    for year in [2021, 2022, 2023, 2024, 2025]:
        print(f"\nBuilding {year}_top10.md...", file=sys.stderr)
        content = build_yearly_report(year, post21_combos, post21_lookup, funding_df)
        out_path = OUT_DIR / f"{year}_top10.md"
        out_path.write_text(content)
        generated.append(str(out_path.name))
        print(f"  Written: {out_path} ({len(content)} bytes)", file=sys.stderr)

    # Pre-2021 regime reports
    for period in ["pre21_bear", "pre21_bull", "pre21_covid"]:
        print(f"\nBuilding {period}_top10.md...", file=sys.stderr)
        content = build_pre21_report(period, adjusted_lookup, funding_df)
        out_path = OUT_DIR / f"{period}_top10.md"
        out_path.write_text(content)
        generated.append(str(out_path.name))
        size = len(content)
        print(f"  Written: {out_path} ({size} bytes)", file=sys.stderr)

    # Index file
    index_content = build_index_md(generated)
    (OUT_DIR / "README.md").write_text(index_content)
    print(f"\nWritten: {OUT_DIR}/README.md", file=sys.stderr)

    print(f"\nSUMMARY: Generated {len(generated)} reports + README → {OUT_DIR}", file=sys.stdout)


if __name__ == "__main__":
    main()
