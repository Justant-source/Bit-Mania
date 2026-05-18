#!/usr/bin/env python3
"""
p3_yearly_sanity.py — 상위 5 candidate 연도별 수익/MDD breakdown

top20.md에서 상위 5 combo_id를 읽고 각각 연도별 monthly_returns.csv를 분석.
combo_<id>/monthly_returns.csv 경로에 있어야 함.

Usage:
    python3 p3_yearly_sanity.py \
        --results-dir /result/p1_optimization \
        --top-n 5 \
        --output /result/p3_validation/yearly_breakdown.md
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from pathlib import Path

CHAMPION_HP = {
    'st_factor': 2.5, 'st_period': 6, 'fast_ema_len': 7,
    'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0, 'sl_margin_pct': 0.0,
}


def load_top_n(db_path: Path, n: int) -> list[dict]:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, atr_mult, sl_margin_pct, cagr, mdd, sharpe, trades, multiplier "
        "FROM jobs WHERE status='done' AND multiplier IS NOT NULL "
        "ORDER BY multiplier DESC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_monthly_returns(results_dir: Path, combo_id: int) -> dict[int, list[float]]:
    path = results_dir / f"combo_{combo_id}" / "monthly_returns.csv"
    if not path.exists():
        return {}
    yearly_pnl: dict[int, list[float]] = defaultdict(list)
    try:
        with open(path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'month' in row and 'pnl_usdt' in row:
                    # format: month=YYYY-MM, pnl_usdt=absolute PnL
                    yr = int(row['month'][:4])
                    yearly_pnl[yr].append(float(row['pnl_usdt']))
                elif 'year' in row and 'month_return_pct' in row:
                    yr = int(row['year'])
                    yearly_pnl[yr].append(float(row['month_return_pct']))
                elif 'date' in row and 'return' in row:
                    yr = int(row['date'][:4])
                    yearly_pnl[yr].append(float(row['return']))
    except Exception:
        pass
    return dict(yearly_pnl)


def yearly_stats(monthly_by_year: dict[int, list[float]]) -> dict[int, dict]:
    result = {}
    for yr, monthly in sorted(monthly_by_year.items()):
        annual_pnl = sum(monthly)
        # MDD from cumulative PnL sequence
        mdd = 0.0
        peak = 0.0
        running = 0.0
        for m in monthly:
            running += m
            if running > peak:
                peak = running
            dd = running - peak
            if dd < mdd:
                mdd = dd
        result[yr] = {'annual_pnl': annual_pnl, 'mdd_usdt': mdd, 'n_months': len(monthly)}
    return result


def write_report(candidates: list[dict], results_dir: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Phase 3 — 연도별 수익/MDD Sanity Check",
        "",
        f"상위 {len(candidates)} candidates × 연도별 분석",
        "",
    ]

    for rank, c in enumerate(candidates, 1):
        is_champ = all(abs(c.get(k, -999) - v) < 0.001 for k, v in CHAMPION_HP.items())
        champ_tag = " (**챔피언 HP**)" if is_champ else ""
        sl_label = f"sl={c['sl_margin_pct']:.0f}%" if c['sl_margin_pct'] < 0 else "sl=off"
        lines += [
            f"## Rank #{rank}: combo_id={c['combo_id']}{champ_tag}",
            "",
            f"HP: st_factor={c['st_factor']} | st_period={c['st_period']} | "
            f"fast_ema={c['fast_ema_len']} | slow_ema={c['slow_ema_len']} | "
            f"dir_ema={c['direction_ema_len']} | atr_mult={c['atr_mult']} | {sl_label}",
            "",
            f"전체기간: mult={c['multiplier']:.2f}x | CAGR={c['cagr']:.1f}% | "
            f"MDD={c['mdd']:.1f}% | Sharpe={c['sharpe']:.3f} | trades={c['trades']}",
            "",
            "| 연도 | 연간수익($) | MDD($) | 월수 |",
            "|---|---|---|---|",
        ]

        monthly_by_year = load_monthly_returns(results_dir, c['combo_id'])
        if monthly_by_year:
            stats = yearly_stats(monthly_by_year)
            for yr, s in sorted(stats.items()):
                lines.append(
                    f"| {yr} | ${s['annual_pnl']:,.0f} | ${s['mdd_usdt']:,.0f} | {s['n_months']} |"
                )
        else:
            lines.append("| (monthly_returns.csv 없음 — 전체기간 통계만 사용 가능) | — | — | — |")

        lines.append("")

    output.write_text('\n'.join(lines) + '\n')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--results-dir', type=str, default='/result/p1_optimization')
    p.add_argument('--top-n', type=int, default=5)
    p.add_argument('--output', type=str, default='/result/p3_validation/yearly_breakdown.md')
    args = p.parse_args()

    results_dir = Path(args.results_dir)
    db_path = results_dir / 'queue.sqlite3'
    output = Path(args.output)

    if not db_path.exists():
        print(f"[ERROR] DB not found: {db_path}")
        return 1

    candidates = load_top_n(db_path, args.top_n)
    print(f"Loaded top-{len(candidates)} candidates")
    for c in candidates:
        print(f"  combo={c['combo_id']} mult={c['multiplier']:.2f}x cagr={c['cagr']:.1f}% "
              f"mdd={c['mdd']:.1f}%")

    write_report(candidates, results_dir, output)
    print(f"Report written: {output}")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
